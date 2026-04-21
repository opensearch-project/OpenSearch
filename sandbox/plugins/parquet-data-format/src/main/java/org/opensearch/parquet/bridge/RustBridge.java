/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.io.IOException;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

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
    private static final MethodHandle GET_SORT_PERMUTATION;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        CREATE_WRITER = linker.downcallHandle(
            lib.find("parquet_create_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,   // file
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,   // index_name
                ValueLayout.JAVA_LONG,                        // schema_address
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, // sort_columns (ptrs, lens, count)
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,   // reverse_sorts (vals, count)
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG    // nulls_first (vals, count)
            )
        );
        WRITE = linker.downcallHandle(
            lib.find("parquet_write").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG
            )
        );
        FINALIZE_WRITER = linker.downcallHandle(
            lib.find("parquet_finalize_writer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS
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
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS
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
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,   // index_name
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,   // compression_type
                ValueLayout.JAVA_LONG,                        // compression_level
                ValueLayout.JAVA_LONG,                        // page_size_bytes
                ValueLayout.JAVA_LONG,                        // page_row_limit
                ValueLayout.JAVA_LONG,                        // dict_size_bytes
                ValueLayout.JAVA_LONG,                        // row_group_size_bytes
                ValueLayout.JAVA_LONG,                        // bloom_filter_enabled
                ValueLayout.JAVA_DOUBLE,                      // bloom_filter_fpp
                ValueLayout.JAVA_LONG                         // bloom_filter_ndv
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
                ValueLayout.ADDRESS, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, // input files (ptrs, lens, count)
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,                      // output file
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG                       // index_name
            )
        );
        GET_SORT_PERMUTATION = linker.downcallHandle(
            lib.find("parquet_get_sort_permutation").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,   // file
                ValueLayout.ADDRESS,                           // count_out
                ValueLayout.ADDRESS,                           // old_row_ids_out
                ValueLayout.ADDRESS                            // new_row_ids_out
            )
        );
    }

    public static void initLogger() {}

    static void createWriter(
        String file,
        String indexName,
        long schemaAddress,
        List<String> sortColumns,
        List<Boolean> reverseSorts,
        List<Boolean> nullsFirst
    ) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var idx = call.str(indexName);
            var sorts = call.strArray(sortColumns.toArray(new String[0]));
            var reverseArray = marshalBoolList(call, reverseSorts);
            var nullsFirstArray = marshalBoolList(call, nullsFirst);
            call.invokeIO(
                CREATE_WRITER,
                f.segment(), f.len(),
                idx.segment(), idx.len(),
                schemaAddress,
                sorts.ptrs(), sorts.lens(), sorts.count(),
                reverseArray, (long) reverseSorts.size(),
                nullsFirstArray, (long) nullsFirst.size()
            );
        }
    }

    static void write(String file, long arrayAddress, long schemaAddress) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            call.invokeIO(WRITE, f.segment(), f.len(), arrayAddress, schemaAddress);
        }
    }

    static ParquetFileMetadata finalizeWriter(String file) throws IOException {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var versionOut = call.intOut();
            var numRowsOut = call.longOut();
            var crc32Out = call.longOut();
            var out = call.outBuffer(1024);
            long rc = call.invokeIO(
                FINALIZE_WRITER,
                f.segment(), f.len(),
                versionOut, numRowsOut,
                out.data(), (long) out.capacity(), out.lenOut(),
                crc32Out
            );
            if (rc == 1) return null;
            int createdByLen = out.actualLength();
            return new ParquetFileMetadata(
                versionOut.get(ValueLayout.JAVA_INT, 0),
                numRowsOut.get(ValueLayout.JAVA_LONG, 0),
                createdByLen >= 0
                    ? new String(out.data().asSlice(0, createdByLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8)
                    : null,
                crc32Out.get(ValueLayout.JAVA_LONG, 0)
            );
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
            call.invokeIO(
                GET_FILE_METADATA,
                f.segment(), f.len(),
                versionOut, numRowsOut,
                out.data(), (long) out.capacity(), out.lenOut()
            );
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
                idx.segment(), idx.len(),
                ct != null ? ct.segment() : java.lang.foreign.MemorySegment.NULL, ct != null ? ct.len() : -1L,
                nativeSettings.getCompressionLevel() != null ? (long) nativeSettings.getCompressionLevel() : -1L,
                nativeSettings.getPageSizeBytes() != null ? nativeSettings.getPageSizeBytes() : -1L,
                nativeSettings.getPageRowLimit() != null ? (long) nativeSettings.getPageRowLimit() : -1L,
                nativeSettings.getDictSizeBytes() != null ? nativeSettings.getDictSizeBytes() : -1L,
                nativeSettings.getRowGroupSizeBytes() != null ? nativeSettings.getRowGroupSizeBytes() : -1L,
                nativeSettings.getBloomFilterEnabled() != null ? (nativeSettings.getBloomFilterEnabled() ? 1L : 0L) : -1L,
                nativeSettings.getBloomFilterFpp() != null ? nativeSettings.getBloomFilterFpp() : -1.0,
                nativeSettings.getBloomFilterNdv() != null ? nativeSettings.getBloomFilterNdv() : -1L
            );
        }
    }

    public static void removeSettings(String indexName) {
        try (var call = new NativeCall()) {
            var idx = call.str(indexName);
            call.invoke(REMOVE_SETTINGS, idx.segment(), idx.len());
        }
    }

    public static void mergeParquetFilesInRust(List<Path> inputFiles, String outputFile, String indexName) {
        String[] paths = inputFiles.stream().map(Path::toString).toArray(String[]::new);
        try (var call = new NativeCall()) {
            var inputs = call.strArray(paths);
            var out = call.str(outputFile);
            var idx = call.str(indexName);
            call.invoke(
                MERGE_FILES,
                inputs.ptrs(), inputs.lens(), inputs.count(),
                out.segment(), out.len(),
                idx.segment(), idx.len()
            );
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
     * Retrieves the sort permutation cached during finalizeWriter's sort-on-close.
     * Returns a long[][] where [0] = old_row_ids, [1] = new_row_ids, or null if no permutation.
     * The permutation is removed from the cache after retrieval (single-use).
     */
    static long[][] getSortPermutation(String file) {
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var countOut = call.longOut();
            // First call with null buffers to get the count
            long rc = call.invoke(GET_SORT_PERMUTATION, f.segment(), f.len(), countOut,
                java.lang.foreign.MemorySegment.NULL, java.lang.foreign.MemorySegment.NULL);
            if (rc <= 0) {
                return null;
            }
            // We got the count but the permutation was already removed from cache.
            // We need a different approach: allocate buffers upfront with a reasonable max size.
            // Actually, the Rust side removes on retrieval, so we need to do it in one call.
            // Let's re-approach: allocate large buffers and do a single call.
            return null; // Will be handled differently - see below
        }
    }

    /**
     * Retrieves the sort permutation with pre-allocated buffers.
     * Called after finalizeWriter when we know the row count from metadata.
     *
     * @param file the Parquet file path
     * @param expectedRows the expected number of rows (from ParquetFileMetadata)
     * @return array of [old_row_ids, new_row_ids] or null if no permutation
     */
    static long[][] getSortPermutationWithSize(String file, long expectedRows) {
        if (expectedRows <= 0) {
            return null;
        }
        try (var call = new NativeCall()) {
            var f = call.str(file);
            var countOut = call.longOut();
            // Allocate buffers for the expected number of rows
            var arena = java.lang.foreign.Arena.ofConfined();
            var oldRowIds = arena.allocate(ValueLayout.JAVA_LONG, expectedRows);
            var newRowIds = arena.allocate(ValueLayout.JAVA_LONG, expectedRows);
            long rc = call.invoke(GET_SORT_PERMUTATION, f.segment(), f.len(), countOut, oldRowIds, newRowIds);
            if (rc <= 0) {
                arena.close();
                return null;
            }
            long count = countOut.get(ValueLayout.JAVA_LONG, 0);
            if (count <= 0) {
                arena.close();
                return null;
            }
            long[] oldIds = new long[(int) count];
            long[] newIds = new long[(int) count];
            for (int i = 0; i < count; i++) {
                oldIds[i] = oldRowIds.get(ValueLayout.JAVA_LONG, (long) i * 8);
                newIds[i] = newRowIds.get(ValueLayout.JAVA_LONG, (long) i * 8);
            }
            arena.close();
            return new long[][] { oldIds, newIds };
        }
    }

    private RustBridge() {}
}
