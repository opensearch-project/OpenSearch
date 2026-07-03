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
import org.opensearch.plugins.NativeStoreHandle;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
     * Creates a DatafusionReader for the given shard directory and per-segment files (no encryption).
     * <p>
     * Each {@link WriterFileSet} is narrowed to a {@link MonoFileWriterSet} — Parquet
     * produces exactly one file per segment. This fails fast if a multi-file set is
     * encountered, preventing silent correctness bugs in the native reader.
     *
     * @param directoryPath shard data directory
     * @param writerFileSets the per-segment file sets from the catalog snapshot
     * @param dataformatAwareStoreHandle per-format native store handle (null on hot, live on warm)
     * @param sortFields index.sort.field values (empty list when the index has no sort). Parallel to {@code sortOrders}.
     * @param sortOrders index.sort.order values ("asc"/"desc"), parallel to {@code sortFields}.
     */
    /**
     * Creates a DatafusionReader without encryption (plaintext files).
     */
    public DatafusionReader(String directoryPath, Collection<WriterFileSet> writerFileSets, NativeStoreHandle dataformatAwareStoreHandle) {
        this(directoryPath, writerFileSets, dataformatAwareStoreHandle, List.of(), List.of(), Map.of(), Map.of());
    }

    /**
     * Creates a DatafusionReader with per-file footer keys and AAD prefixes for PME-encrypted files.
     *
     * @param directoryPath  shard data directory
     * @param writerFileSets the per-segment file sets from the catalog snapshot
     * @param dataformatAwareStoreHandle per-format native store handle (null on hot, live on warm)
     * @param sortFields index.sort.field values (empty list when the index has no sort). Parallel to {@code sortOrders}.
     * @param sortOrders index.sort.order values ("asc"/"desc"), parallel to {@code sortFields}.
     * @param fileFooterKeys map from absolute file path to 32-byte AES-GCM footer key
     * @param fileAadPrefixes map from absolute file path to binary AAD prefix bytes
     */
    public DatafusionReader(
        String directoryPath,
        Collection<WriterFileSet> writerFileSets,
        NativeStoreHandle dataformatAwareStoreHandle,
        List<String> sortFields,
        List<String> sortOrders,
        Map<String, byte[]> fileFooterKeys,
        Map<String, byte[]> fileAadPrefixes
    ) {
        this.directoryPath = directoryPath;
        List<MonoFileWriterSet> segments;
        if (writerFileSets == null || writerFileSets.isEmpty()) {
            segments = List.of();
        } else {
            segments = writerFileSets.stream().map(MonoFileWriterSet::from).toList();
        }
        readerHandle = new ReaderHandle(directoryPath, segments, dataformatAwareStoreHandle, sortFields, sortOrders, fileFooterKeys, fileAadPrefixes);
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
