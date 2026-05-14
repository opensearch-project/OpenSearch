/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.nativelib.ReaderHandle;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * DataFusion reader for JNI operations.
 * <p>
 * Each reader represents a point-in-time snapshot of parquet/arrow files for a shard.
 * Created from a catalog snapshot during refresh; closed when the associated catalog
 * snapshot is removed.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReader implements Closeable {

    private static final Logger logger = LogManager.getLogger(DatafusionReader.class);
    private final String directoryPath;
    private final ReaderHandle readerHandle;

    /**
     * Creates a DatafusionReader for the given shard directory and per-segment files.
     * <p>
     * Each {@link WriterFileSet} is narrowed to a {@link MonoFileWriterSet} — Parquet
     * produces exactly one file per segment. This fails fast if a multi-file set is
     * encountered, preventing silent correctness bugs in the native reader.
     *
     * @param directoryPath shard data directory
     * @param writerFileSets the per-segment file sets from the catalog snapshot
     */
    public DatafusionReader(String directoryPath, Collection<WriterFileSet> writerFileSets) {
        this.directoryPath = directoryPath;
        List<MonoFileWriterSet> segments;
        if (writerFileSets == null || writerFileSets.isEmpty()) {
            segments = List.of();
        } else {
            segments = writerFileSets.stream().map(MonoFileWriterSet::from).toList();
        }
        readerHandle = new ReaderHandle(directoryPath, segments);
    }

    /**
     * Wraps a pre-existing native reader pointer (test only).
     * The caller retains ownership — this reader will NOT close the handle.
     */
    DatafusionReader(long nativePtr) {
        this.directoryPath = "";
        this.readerHandle = ReaderHandle.wrap(nativePtr);
    }

    @Override
    public void close() throws IOException {
        readerHandle.close();
        logger.debug("DatafusionReader closed for [{}]", directoryPath);
    }

    /**
     * Returns the type-safe handle to the native reader.
     * Callers should hold this reference and call
     * {@link ReaderHandle#getPointer()} only at JNI invocation time.
     */
    public ReaderHandle getReaderHandle() {
        return readerHandle;
    }
}
