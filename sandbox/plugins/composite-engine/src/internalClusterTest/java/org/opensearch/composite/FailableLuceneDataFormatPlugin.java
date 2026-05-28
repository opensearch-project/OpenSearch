/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.opensearch.be.lucene.LuceneDataFormat;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.be.lucene.index.LuceneCommitter;
import org.opensearch.be.lucene.index.LuceneIndexingExecutionEngine;
import org.opensearch.be.lucene.index.LuceneWriter;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.commit.Committer;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test-only plugin that wraps {@link LucenePlugin} and injects deterministic I/O failures
 * inside Lucene's {@link Directory} layer — simulating real-world disk faults that surface
 * mid-{@code IndexWriter.addDocument}. Mirrors the failure-injection model of the original
 * stub but for Lucene, so tests can exercise checkpoint/promotion paths where a Lucene
 * secondary writer fails at the directory level.
 *
 * <p>Failure isolation: only writes from this plugin's writers are affected. Each new
 * Lucene writer creates its own injected directory; failures are not cross-writer.
 *
 * <p>Usage:
 * <pre>{@code
 *   nodePlugins() -> include FailableLuceneDataFormatPlugin.class instead of LucenePlugin.class
 *   FailableLuceneDataFormatPlugin.failOnNthWrite(1);  // next write op throws IOException
 *   ... index docs ...
 *   FailableLuceneDataFormatPlugin.clearFailure();
 * }</pre>
 */
public class FailableLuceneDataFormatPlugin extends LucenePlugin {

    /**
     * Counts down on each Directory write op; when it hits zero the next op throws.
     * "Disabled" means "set to a large enough number that we won't realistically reach zero"
     * — see {@link #DISABLED}.
     */
    private static final int DISABLED = 1000;
    private static final AtomicInteger countdownToFailure = new AtomicInteger(DISABLED);

    /**
     * After {@code n} successful Directory write operations across all Lucene writers in
     * this JVM, the (n+1)th will throw {@code IOException}.
     */
    public static void failOnNthWrite(int n) {
        countdownToFailure.set(n);
    }

    /** Disables fault injection by setting the countdown to {@link #DISABLED}. */
    public static void clearFailure() {
        countdownToFailure.set(DISABLED);
    }

    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig indexingEngineConfig) {
        Committer committer = indexingEngineConfig.committer();
        if (committer instanceof LuceneCommitter luceneCommitter) {
            return new FaultInjectingLuceneEngine(
                (LuceneDataFormat) getDataFormat(),
                luceneCommitter,
                indexingEngineConfig.mapperService(),
                indexingEngineConfig.store()
            );
        }
        throw new IllegalStateException(
            "FailableLuceneDataFormatPlugin requires a LuceneCommitter but got: "
                + (committer != null ? committer.getClass().getName() : "null")
        );
    }

    /**
     * Subclass of the production engine that swaps in a {@link FaultInjectingLuceneWriter}
     * for every per-generation writer. Everything else flows through unchanged.
     */
    private static final class FaultInjectingLuceneEngine extends LuceneIndexingExecutionEngine {

        FaultInjectingLuceneEngine(
            LuceneDataFormat dataFormat,
            LuceneCommitter luceneCommitter,
            org.opensearch.index.mapper.MapperService mapperService,
            org.opensearch.index.store.Store store
        ) {
            super(dataFormat, luceneCommitter, mapperService, store);
        }

        @Override
        protected LuceneWriter buildLuceneWriter(
            long writerGeneration,
            long mappingVersion,
            LuceneDataFormat dataFormat,
            Path baseDirectory,
            Analyzer analyzer,
            Codec codec,
            Sort indexSort,
            Set<LuceneWriter> registry
        ) throws IOException {
            return new FaultInjectingLuceneWriter(
                writerGeneration,
                mappingVersion,
                dataFormat,
                baseDirectory,
                analyzer,
                codec,
                indexSort,
                registry
            );
        }
    }

    /**
     * LuceneWriter subclass that wraps the underlying {@link MMapDirectory} with a
     * {@link FaultInjectingDirectory}. The fault directory triggers an {@code IOException}
     * inside any in-flight {@code IndexWriter.addDocument} once {@link #countdownToFailure}
     * reaches zero.
     */
    private static final class FaultInjectingLuceneWriter extends LuceneWriter {
        FaultInjectingLuceneWriter(
            long writerGeneration,
            long mappingVersion,
            LuceneDataFormat dataFormat,
            Path baseDirectory,
            Analyzer analyzer,
            Codec codec,
            Sort indexSort,
            Set<LuceneWriter> registry
        ) throws IOException {
            super(writerGeneration, mappingVersion, dataFormat, baseDirectory, analyzer, codec, indexSort, registry);
        }

        @Override
        protected Directory createDirectory(Path tempDirectory) throws IOException {
            return new FaultInjectingDirectory(new MMapDirectory(tempDirectory));
        }

        /**
         * Force a flush after every 2nd doc so the {@link FaultInjectingDirectory} sees a
         * write op at predictable doc boundaries. Mirrors the existing
         * {@code newWriterWithMaxBufferedDocs} pattern in the codebase: very large RAM buffer
         * (1024 MB) so doc count is the sole flush trigger, with {@code maxBufferedDocs=2}
         * (the smallest legal value).
         */
        @Override
        protected double ramBufferSizeMB() {
            return 1024.0;
        }

        @Override
        protected int maxBufferedDocs() {
            return 2;
        }
    }

    /**
     * {@link FilterDirectory} that throws {@code IOException} from any {@code createOutput}
     * once the static countdown reaches zero. This is the path Lucene's IndexWriter uses
     * to write segment files, so failures here propagate up as IOExceptions inside
     * {@code addDocument} or other IndexWriter operations.
     */
    private static final class FaultInjectingDirectory extends FilterDirectory {

        FaultInjectingDirectory(Directory delegate) {
            super(delegate);
        }

        @Override
        public IndexOutput createOutput(String name, IOContext context) throws IOException {
            maybeFail(name);
            return super.createOutput(name, context);
        }

        @Override
        public IndexOutput createTempOutput(String prefix, String suffix, IOContext context) throws IOException {
            maybeFail(prefix + "/" + suffix);
            return super.createTempOutput(prefix, suffix, context);
        }

        private static void maybeFail(String name) throws IOException {
            if (countdownToFailure.getAndDecrement() == 0) {
                throw new IOException("injected lucene directory failure on " + name + " (test-only)");
            }
        }
    }
}
