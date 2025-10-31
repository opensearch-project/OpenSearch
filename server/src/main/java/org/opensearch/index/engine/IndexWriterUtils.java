/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.opensearch.Version;
import org.opensearch.common.Booleans;
import org.opensearch.common.lucene.LoggerInfoStream;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.Assertions;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CriteriaBasedCodec;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.shard.OpenSearchMergePolicy;

import java.io.IOException;
import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * Utility class for creating IndexWriter and IndexWriterConfig.
 *
 */
public final class IndexWriterUtils {
    private IndexWriterUtils() {}

    public static IndexWriter createWriter(
        Directory directory,
        MergeScheduler mergeScheduler,
        Boolean commitOnClose,
        IndexWriterConfig.OpenMode openMode,
        CombinedDeletionPolicy deletionPolicy,
        SoftDeletesPolicy softDeletesPolicy,
        EngineConfig engineConfig,
        Logger logger,
        String associatedCriteria
    ) throws IOException {
        IndexWriterConfig config = getIndexWriterConfig(
            mergeScheduler,
            commitOnClose,
            openMode,
            deletionPolicy,
            softDeletesPolicy,
            engineConfig,
            logger,
            associatedCriteria
        );

        return createWriter(directory, config);
    }

    public static IndexWriter createWriter(Directory directory, IndexWriterConfig config) throws IOException {
        if (Assertions.ENABLED) {
            return new AssertingIndexWriter(directory, config);
        } else {
            return new IndexWriter(directory, config);
        }
    }

    public static IndexWriterConfig getIndexWriterConfig(
        MergeScheduler mergeScheduler,
        Boolean commitOnClose,
        IndexWriterConfig.OpenMode openMode,
        CombinedDeletionPolicy deletionPolicy,
        SoftDeletesPolicy softDeletesPolicy,
        EngineConfig engineConfig,
        Logger logger,
        String associatedCriteria
    ) {
        final IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
        iwc.setCommitOnClose(commitOnClose);
        iwc.setOpenMode(openMode);
        if (openMode == IndexWriterConfig.OpenMode.CREATE) {
            iwc.setIndexCreatedVersionMajor(engineConfig.getIndexSettings().getIndexVersionCreated().luceneVersion.major);
        }

        if (deletionPolicy != null) {
            // For child IndexWriter, we are not setting deletion policy.
            iwc.setIndexDeletionPolicy(deletionPolicy);
        }

        // with tests.verbose, lucene sets this up: plumb to align with filesystem stream
        boolean verbose = false;
        try {
            verbose = Boolean.parseBoolean(System.getProperty("tests.verbose"));
        } catch (Exception ignore) {}
        iwc.setInfoStream(verbose ? InfoStream.getDefault() : new LoggerInfoStream(logger));
        iwc.setMergeScheduler(mergeScheduler);
        // Give us the opportunity to upgrade old segments while performing
        // background merges
        MergePolicy mergePolicy = engineConfig.getMergePolicy();
        // always configure soft-deletes field so an engine with soft-deletes disabled can open a Lucene index with soft-deletes.
        iwc.setSoftDeletesField(Lucene.SOFT_DELETES_FIELD);
        mergePolicy = new RecoverySourcePruneMergePolicy(
            SourceFieldMapper.RECOVERY_SOURCE_NAME,
            softDeletesPolicy::getRetentionQuery,
            new SoftDeletesRetentionMergePolicy(
                Lucene.SOFT_DELETES_FIELD,
                softDeletesPolicy::getRetentionQuery,
                new PrunePostingsMergePolicy(mergePolicy, IdFieldMapper.NAME)
            )
        );
        boolean shuffleForcedMerge = Booleans.parseBoolean(System.getProperty("opensearch.shuffle_forced_merge", Boolean.TRUE.toString()));
        if (shuffleForcedMerge) {
            // We wrap the merge policy for all indices even though it is mostly useful for time-based indices
            // but there should be no overhead for other type of indices so it's simpler than adding a setting
            // to enable it.
            mergePolicy = new ShuffleForcedMergePolicy(mergePolicy);
        }
        if (engineConfig.getIndexSettings().isMergeOnFlushEnabled()) {
            final long maxFullFlushMergeWaitMillis = engineConfig.getIndexSettings().getMaxFullFlushMergeWaitTime().millis();
            if (maxFullFlushMergeWaitMillis > 0) {
                iwc.setMaxFullFlushMergeWaitMillis(maxFullFlushMergeWaitMillis);
                final Optional<UnaryOperator<MergePolicy>> mergeOnFlushPolicy = engineConfig.getIndexSettings().getMergeOnFlushPolicy();
                if (mergeOnFlushPolicy.isPresent()) {
                    mergePolicy = mergeOnFlushPolicy.get().apply(mergePolicy);
                }
            }
        } else {
            // Disable merge on refresh
            iwc.setMaxFullFlushMergeWaitMillis(0);
        }
        iwc.setCheckPendingFlushUpdate(engineConfig.getIndexSettings().isCheckPendingFlushEnabled());
        iwc.setMergePolicy(new OpenSearchMergePolicy(mergePolicy));
        iwc.setSimilarity(engineConfig.getSimilarity());
        iwc.setRAMBufferSizeMB(engineConfig.getIndexingBufferSize().getMbFrac());
        if (engineConfig.getIndexSettings().isContextAwareEnabled()) {
            iwc.setCodec(new CriteriaBasedCodec(engineConfig.getCodec(), associatedCriteria));
        } else {
            iwc.setCodec(engineConfig.getCodec());
        }

        iwc.setUseCompoundFile(engineConfig.useCompoundFile());
        if (engineConfig.getIndexSort() != null) {
            iwc.setIndexSort(engineConfig.getIndexSort());
            if (engineConfig.getIndexSettings().getIndexVersionCreated().onOrAfter(Version.V_3_2_0)) {
                iwc.setParentField(Lucene.PARENT_FIELD);
            }
        }
        if (engineConfig.getLeafSorter() != null) {
            iwc.setLeafSorter(engineConfig.getLeafSorter()); // The default segment search order
        }
        IndexSettings indexSettings = engineConfig.getIndexSettings();
        if (indexSettings.isDocumentReplication() == false
            && (indexSettings.isSegRepLocalEnabled() || indexSettings.isRemoteStoreEnabled())) {
            assert null != engineConfig.getIndexReaderWarmer();
            iwc.setMergedSegmentWarmer(engineConfig.getIndexReaderWarmer());
        }
        return iwc;
    }

    /**
     * Internal Asserting Index Writer
     *
     * @opensearch.internal
     */
    private static class AssertingIndexWriter extends IndexWriter {
        AssertingIndexWriter(Directory d, IndexWriterConfig conf) throws IOException {
            super(d, conf);
        }

        @Override
        public long updateDocuments(Term delTerm, Iterable<? extends Iterable<? extends IndexableField>> docs) {
            throw new AssertionError("must not hard update documents");
        }

        @Override
        public long tryDeleteDocument(IndexReader readerIn, int docID) {
            assert false : "#tryDeleteDocument is not supported. See Lucene#DirectoryReaderWithAllLiveDocs";
            throw new UnsupportedOperationException();
        }
    }
}
