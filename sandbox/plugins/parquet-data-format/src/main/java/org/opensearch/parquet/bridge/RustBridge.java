/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.index.engine.dataformat.PackedRowIdMapping;
import org.opensearch.index.engine.dataformat.PackedSingleGenRowIdMapping;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * FFM bridge to the native Rust parquet writer library.
 */
public class RustBridge {

    private static final MethodHandle CREATE_WRITER;
    private static final MethodHandle WRITE;
    private static final MethodHandle FINALIZE_WRITER;
    private static final MethodHandle SYNC_TO_DISK;
    private static final MethodHandle GET_FILE_METADATA;
    private static final MethodHandle GET_FILTERED_BYTES;
    private static final MethodHandle ON_SETTINGS_UPDATE;
    private static final MethodHandle REMOVE_SETTINGS;
    private static final MethodHandle MERGE_FILES;
    private static final MethodHandle FREE_MERGE_RESULT;
    private static final MethodHandle READ_AS_JSON;
    private static final MethodHandle FREE_ROW_ID_MAPPING;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        CREATE_WRITER = linker.downcallHandle(
            lib.find("parquet_create_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,   // file
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,   // index_name
                ValueLayout.JAVA_LONG,                        // schema_address
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG, // sort_columns (ptrs, lens, count)
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,   // reverse_sorts (vals, count)
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,   // nulls_first (vals, count)
                ValueLayout.JAVA_LONG    // writer_generation
            )
        );
        WRITE = linker.downcallHandle(
            lib.find("parquet_write").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );
        FINALIZE_WRITER = linker.downcallHandle(
            lib.find("parquet_finalize_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,                           // sort_perm_ptr_out
                ValueLayout.ADDRESS                            // sort_perm_len_out
            )
        );
        SYNC_TO_DISK = linker.downcallHandle(
            lib.find("parquet_sync_to_disk").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        GET_FILE_METADATA = linker.downcallHandle(
            lib.find("parquet_get_file_metadata").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS
            )
        );
        GET_FILTERED_BYTES = linker.downcallHandle(
            lib.find("parquet_get_filtered_native_bytes_used").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        ON_SETTINGS_UPDATE = linker.downcallHandle(
            lib.find("parquet_on_settings_update").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,   // index_name
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,   // compression_type
                ValueLayout.JAVA_LONG,                        // compression_level
                ValueLayout.JAVA_LONG,                        // page_size_bytes
                ValueLayout.JAVA_LONG,                        // page_row_limit
                ValueLayout.JAVA_LONG,                        // dict_size_bytes
                ValueLayout.JAVA_LONG,                        // bloom_filter_enabled
                ValueLayout.JAVA_DOUBLE,                      // bloom_filter_fpp
                ValueLayout.JAVA_LONG,                        // bloom_filter_ndv
                ValueLayout.JAVA_LONG,                        // sort_in_memory_threshold_bytes
                ValueLayout.JAVA_LONG,                        // sort_batch_size
                ValueLayout.JAVA_LONG,                        // row_group_max_rows
                ValueLayout.JAVA_LONG,                        // merge_batch_size
                ValueLayout.JAVA_LONG,                        // merge_rayon_threads
                ValueLayout.JAVA_LONG                         // merge_io_threads
            )
        );
        REMOVE_SETTINGS = linker.downcallHandle(
            lib.find("parquet_remove_settings").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        MERGE_FILES = linker.downcallHandle(
            lib.find("parquet_merge_files").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,  // input files (ptrs, lens, count)
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,  // output file
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,  // index_name
                ValueLayout.ADDRESS,    // version_out
                ValueLayout.ADDRESS,    // num_rows_out
                ValueLayout.ADDRESS,    // created_by_buf
                ValueLayout.JAVA_LONG,  // created_by_buf_len
                ValueLayout.ADDRESS,    // created_by_len_out
                ValueLayout.ADDRESS,    // crc32_out
                ValueLayout.ADDRESS,    // out_mapping_ptr
                ValueLayout.ADDRESS,    // out_mapping_len
                ValueLayout.ADDRESS,    // out_gen_keys_ptr
                ValueLayout.ADDRESS,    // out_gen_offsets_ptr
                ValueLayout.ADDRESS,    // out_gen_sizes_ptr
                ValueLayout.ADDRESS     // out_gen_count
            )
        );
        FREE_MERGE_RESULT = linker.downcallHandle(
            lib.find("parquet_free_merge_result").orElseThrow(),
            FunctionDescriptor.ofVoid(
                ValueLayout.JAVA_LONG,  // mapping_ptr
                ValueLayout.JAVA_LONG,  // mapping_len
                ValueLayout.JAVA_LONG,  // gen_keys_ptr
                ValueLayout.JAVA_LONG,  // gen_offsets_ptr
                ValueLayout.JAVA_LONG,  // gen_sizes_ptr
                ValueLayout.JAVA_LONG   // gen_count
            )
        );
        READ_AS_JSON = linker.downcallHandle(
            lib.find("parquet_read_as_json").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,   // file
                ValueLayout.ADDRESS,     // out_buf
                ValueLayout.JAVA_LONG,   // buf_capacity
                ValueLayout.ADDRESS      // out_len
            )
        );
        FREE_ROW_ID_MAPPING = linker.downcallHandle(
            lib.find("parquet_free_row_id_mapping").orElseThrow(),
            FunctionDescriptor.ofVoid(
                ValueLayout.JAVA_LONG,                         // mapping_ptr
                ValueLayout.JAVA_LONG                          // mapping_len
            )
        );
    }

    public static void initLogger() {}

    static void createWriter(String file, String indexName, long schemaAddress, ParquetSortConfig sortConfig, long writerGeneration)
        throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var idx = call.str(indexName);
            var sorts = call.strArray(sortConfig.sortColumns().toArray(new String[0]));
            var reverseArray = marshalBoolList(call, sortConfig.reverseSorts());
            var nullsFirstArray = marshalBoolList(call, sortConfig.nullsFirst());
            call.invokeIO(
                CREATE_WRITER,
                f.segment(),
                f.len(),
                idx.segment(),
                idx.len(),
                schemaAddress,
                sorts.ptrs(),
                sorts.lens(),
                sorts.count(),
                reverseArray,
                (long) sortConfig.reverseSorts().size(),
                nullsFirstArray,
                (long) sortConfig.nullsFirst().size(),
                writerGeneration
            );
        }
    }

    static void write(String file, long arrayAddress, long schemaAddress) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            call.invokeIO(WRITE, f.segment(), f.len(), arrayAddress, schemaAddress);
        }
    }

    /**
     * Result of finalizing a writer: metadata + optional row ID mapping.
     */
    record WriterFinalizeResult(ParquetFileMetadata metadata, RowIdMapping rowIdMapping) {}

    static WriterFinalizeResult finalizeWriter(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var crc32Out = call.longOut();
            var out = call.outBuffer(1024);
            var sortPermPtrOut = call.longOut();
            var sortPermLenOut = call.longOut();
            long rc = call.invokeIO(
                FINALIZE_WRITER,
                f.segment(),
                f.len(),
                versionOut,
                numRowsOut,
                out.data(),
                (long) out.capacity(),
                out.lenOut(),
                crc32Out,
                sortPermPtrOut,
                sortPermLenOut
            );
            if (rc == 1) return null;
            int createdByLen = out.actualLength();
            ParquetFileMetadata metadata = new ParquetFileMetadata(
                versionOut.get(ValueLayout.JAVA_INT, 0),
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                createdByLen >= 0
                    ? new String(out.data().asSlice(0, createdByLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)
                    : null,
                crc32Out.get(ValueLayout.JAVA_LONG, 0)
            );

            // Read sort permutation if present
            long permAddr = sortPermPtrOut.get(ValueLayout.JAVA_LONG, 0);
            long permLen = sortPermLenOut.get(ValueLayout.JAVA_LONG, 0);
            RowIdMapping rowIdMapping = null;
            if (permAddr != 0 && permLen > 0) {
                try {
                    long[] mappingArray = MemorySegment.ofAddress(permAddr)
                        .reinterpret(permLen * ValueLayout.JAVA_LONG.byteSize())
                        .toArray(ValueLayout.JAVA_LONG);
                    rowIdMapping = new PackedSingleGenRowIdMapping(mappingArray);
                } finally {
                    NativeCall.invokeVoid(FREE_ROW_ID_MAPPING, permAddr, permLen);
                }
            }

            return new WriterFinalizeResult(metadata, rowIdMapping);
        }
    }

    static void syncToDisk(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            call.invokeIO(SYNC_TO_DISK, f.segment(), f.len());
        }
    }

    public static ParquetFileMetadata getFileMetadata(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var out = call.outBuffer(1024);
            call.invokeIO(GET_FILE_METADATA, f.segment(), f.len(), versionOut, numRowsOut, out.data(), (long) out.capacity(), out.lenOut());
            int createdByLen = out.actualLength();
            return new ParquetFileMetadata(
                versionOut.get(ValueLayout.JAVA_INT, 0),
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                createdByLen >= 0
                    ? new String(out.data().asSlice(0, createdByLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)
                    : null,
                0L
            );
        }
    }

    public static long getFilteredNativeBytesUsed(String pathPrefix) {
        try (var call = new NativeCall()) {
            var p = call.str(pathPrefix);
            return call.invoke(GET_FILTERED_BYTES, p.segment(), p.len());
        }
    }

    public static void onSettingsUpdate(NativeSettings nativeSettings) throws IOException {
        try (var call = new NativeCall()) {
            var idx = call.str(nativeSettings.getIndexName());
            var ct = nativeSettings.getCompressionType() != null ? call.str(nativeSettings.getCompressionType()) : null;
            call.invokeIO(
                ON_SETTINGS_UPDATE,
                idx.segment(),
                idx.len(),
                ct != null ? ct.segment() : java.lang.foreign.MemorySegment.NULL,
                ct != null ? ct.len() : -1L,
                nativeSettings.getCompressionLevel() != null ? (long) nativeSettings.getCompressionLevel() : -1L,
                nativeSettings.getPageSizeBytes() != null ? nativeSettings.getPageSizeBytes() : -1L,
                nativeSettings.getPageRowLimit() != null ? (long) nativeSettings.getPageRowLimit() : -1L,
                nativeSettings.getDictSizeBytes() != null ? nativeSettings.getDictSizeBytes() : -1L,
                nativeSettings.getBloomFilterEnabled() != null ? (nativeSettings.getBloomFilterEnabled() ? 1L : 0L) : -1L,
                nativeSettings.getBloomFilterFpp() != null ? nativeSettings.getBloomFilterFpp() : -1.0,
                nativeSettings.getBloomFilterNdv() != null ? nativeSettings.getBloomFilterNdv() : -1L,
                nativeSettings.getSortInMemoryThresholdBytes() != null ? nativeSettings.getSortInMemoryThresholdBytes() : -1L,
                nativeSettings.getSortBatchSize() != null ? (long) nativeSettings.getSortBatchSize() : -1L,
                nativeSettings.getRowGroupMaxRows() != null ? (long) nativeSettings.getRowGroupMaxRows() : -1L,
                nativeSettings.getMergeBatchSize() != null ? (long) nativeSettings.getMergeBatchSize() : -1L,
                nativeSettings.getMergeRayonThreads() != null ? (long) nativeSettings.getMergeRayonThreads() : -1L,
                nativeSettings.getMergeIoThreads() != null ? (long) nativeSettings.getMergeIoThreads() : -1L
            );
        }
    }

    public static void removeSettings(String indexName) {
        try (var call = new NativeCall()) {
            var idx = call.str(indexName);
            call.invoke(REMOVE_SETTINGS, idx.segment(), idx.len());
        }
    }

    public static MergeFilesResult mergeParquetFilesInRust(List<Path> inputFiles, String outputFile, String indexName) {
        String[] paths = inputFiles.stream().map(Path::toString).toArray(String[]::new);
        try (var call = new NativeCall()) {
            var inputs = call.strArray(paths);
            var out = call.str(outputFile);
            var idx = call.str(indexName);

            // Out-pointers for Parquet file metadata
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var crc32Out = call.longOut();
            var createdByOut = call.outBuffer(1024);

            // Out-pointers for Rust-allocated mapping data
            var outMappingPtr = call.longOut();
            var outMappingLen = call.longOut();
            var outGenKeysPtr = call.longOut();
            var outGenOffsetsPtr = call.longOut();
            var outGenSizesPtr = call.longOut();
            var outGenCount = call.longOut();

            call.invokeIO(
                MERGE_FILES,
                inputs.ptrs(),
                inputs.lens(),
                inputs.count(),
                out.segment(),
                out.len(),
                idx.segment(),
                idx.len(),
                versionOut,
                numRowsOut,
                createdByOut.data(),
                (long) createdByOut.capacity(),
                createdByOut.lenOut(),
                crc32Out,
                outMappingPtr,
                outMappingLen,
                outGenKeysPtr,
                outGenOffsetsPtr,
                outGenSizesPtr,
                outGenCount
            );

            int createdByLen = (int) createdByOut.lenOut().get(ValueLayout.JAVA_LONG, 0);
            ParquetFileMetadata metadata = new ParquetFileMetadata(
                versionOut.get(ValueLayout.JAVA_INT, 0),
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                createdByLen >= 0
                    ? new String(createdByOut.data().asSlice(0, createdByLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)
                    : null,
                crc32Out.get(ValueLayout.JAVA_LONG, 0)
            );

            RowIdMapping rowIdMapping = readAndFreeMergeResult(
                outMappingPtr,
                outMappingLen,
                outGenKeysPtr,
                outGenOffsetsPtr,
                outGenSizesPtr,
                outGenCount
            );

            return new MergeFilesResult(rowIdMapping, metadata);
        } catch (IOException e) {
            throw new UncheckedIOException("Native merge failed", e);
        }
    }

    private static RowIdMapping readAndFreeMergeResult(
        MemorySegment outMappingPtr,
        MemorySegment outMappingLen,
        MemorySegment outGenKeysPtr,
        MemorySegment outGenOffsetsPtr,
        MemorySegment outGenSizesPtr,
        MemorySegment outGenCount
    ) {
        long mappingAddr = outMappingPtr.get(ValueLayout.JAVA_LONG, 0);
        long mappingLen = outMappingLen.get(ValueLayout.JAVA_LONG, 0);
        long genKeysAddr = outGenKeysPtr.get(ValueLayout.JAVA_LONG, 0);
        long genOffsetsAddr = outGenOffsetsPtr.get(ValueLayout.JAVA_LONG, 0);
        long genSizesAddr = outGenSizesPtr.get(ValueLayout.JAVA_LONG, 0);
        long genCount = outGenCount.get(ValueLayout.JAVA_LONG, 0);

        try {
            // Read mapping array (i64[])
            long[] mappingArray = MemorySegment.ofAddress(mappingAddr)
                .reinterpret(mappingLen * ValueLayout.JAVA_LONG.byteSize())
                .toArray(ValueLayout.JAVA_LONG);

            // Read generation keys (i64[]), offsets (i32[]), sizes (i32[])
            long[] genKeys = MemorySegment.ofAddress(genKeysAddr)
                .reinterpret(genCount * ValueLayout.JAVA_LONG.byteSize())
                .toArray(ValueLayout.JAVA_LONG);
            int[] genOffsets = MemorySegment.ofAddress(genOffsetsAddr)
                .reinterpret(genCount * ValueLayout.JAVA_INT.byteSize())
                .toArray(ValueLayout.JAVA_INT);
            int[] genSizes = MemorySegment.ofAddress(genSizesAddr)
                .reinterpret(genCount * ValueLayout.JAVA_INT.byteSize())
                .toArray(ValueLayout.JAVA_INT);

            Map<Long, Integer> offsetMap = new HashMap<>((int) genCount);
            Map<Long, Integer> sizeMap = new HashMap<>((int) genCount);
            for (int i = 0; i < (int) genCount; i++) {
                offsetMap.put(genKeys[i], genOffsets[i]);
                sizeMap.put(genKeys[i], genSizes[i]);
            }

            return new PackedRowIdMapping(mappingArray, offsetMap, sizeMap);
        } finally {
            NativeCall.invokeVoid(FREE_MERGE_RESULT, mappingAddr, mappingLen, genKeysAddr, genOffsetsAddr, genSizesAddr, genCount);
        }
    }

    private static java.lang.foreign.MemorySegment marshalBoolList(NativeCall call, List<Boolean> bools) {
        if (bools == null || bools.isEmpty()) {
            return java.lang.foreign.MemorySegment.NULL;
        }
        var seg = call.buf(bools.size() * 8);
        for (int i = 0; i < bools.size(); i++) {
            seg.setAtIndex(ValueLayout.JAVA_LONG, i, bools.get(i) ? 1L : 0L);
        }
        return seg;
    }

    /**
     * Reads a parquet file and returns its contents as a JSON string.
     */
    public static String readAsJson(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            int bufSize = 10 * 1024 * 1024; // 10MB
            var outBuf = call.buf(bufSize);
            var outLen = call.longOut();
            call.invokeIO(READ_AS_JSON, f.segment(), f.len(), outBuf, (long) bufSize, outLen);
            int len = (int) outLen.get(ValueLayout.JAVA_LONG, 0);
            return new String(outBuf.asSlice(0, len).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
        }
    }

    private RustBridge() {}
}
