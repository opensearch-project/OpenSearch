/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.plugins.NativeStoreHandle;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * Type-safe handle for native reader.
 */
public final class ReaderHandle extends NativeHandle {

    private final boolean ownsPointer;

    /**
     * Creates a reader handle by allocating a native DataFusion reader for the given path
     * and {@link MonoFileWriterSet}s. Each entry represents one parquet segment — a single
     * file paired with its writer generation (sourced from the catalog snapshot).
     *
     * @param path the directory path containing data files
     * @param segments the per-segment file sets to read
     * @param dataformatAwareStoreHandle per-format native store handle (null = local, live = use store pointer)
     * @param sortFields index.sort.field values (or empty if no index sort). Parallel to {@code sortOrders}.
     * @param sortOrders index.sort.order values ("asc"/"desc"), parallel to {@code sortFields}.
     */
    public ReaderHandle(
        String path,
        List<MonoFileWriterSet> segments,
        NativeStoreHandle dataformatAwareStoreHandle,
        List<String> sortFields,
        List<String> sortOrders,
        java.util.Map<String, byte[]> fileFooterKeys,
        java.util.Map<String, byte[]> fileAadPrefixes
    ) {
        super(NativeBridge.createDatafusionReader(path, segments, dataformatAwareStoreHandle, sortFields, sortOrders, fileFooterKeys, fileAadPrefixes));
        this.ownsPointer = true;
    }

    public ReaderHandle(String path, List<MonoFileWriterSet> segments, NativeStoreHandle dataformatAwareStoreHandle) {
        this(path, segments, dataformatAwareStoreHandle, List.of(), List.of(), java.util.Map.of(), java.util.Map.of());
    }

    /**
     * Convenience constructor for sort-only reads (no encryption).
     */
    public ReaderHandle(
        String path,
        List<MonoFileWriterSet> segments,
        NativeStoreHandle dataformatAwareStoreHandle,
        List<String> sortFields,
        List<String> sortOrders
    ) {
        this(path, segments, dataformatAwareStoreHandle, sortFields, sortOrders, java.util.Map.of(), java.util.Map.of());
    }

    /** Wraps an existing pointer without taking ownership. */
    private ReaderHandle(long existingPtr) {
        super(existingPtr);
        this.ownsPointer = false;
    }

    /**
     * Convenience constructor for unencrypted reads from a directory (test / simple read path).
     *
     * @param directoryPath directory containing the Parquet files
     * @param files         file names (relative to directory) to register as segments
     */
    public ReaderHandle(String directoryPath, String[] files) {
        this(directoryPath, fileNamesToSegments(directoryPath, files), null);
    }

    /**
     * Convenience constructor for PME-encrypted reads (test / simple read path).
     *
     * @param directoryPath   directory containing the Parquet files
     * @param files           file names (relative to directory) to register as segments
     * @param footerKeyFiles  file names that have footer keys (parallel with {@code footerKeys})
     * @param footerKeys      raw 32-byte footer keys, one per {@code footerKeyFiles} entry
     * @param aadPrefixFiles  file names that have AAD prefixes (parallel with {@code aadPrefixes})
     * @param aadPrefixes     binary AAD prefix bytes, one per {@code aadPrefixFiles} entry
     */
    public ReaderHandle(
        String directoryPath,
        String[] files,
        String[] footerKeyFiles,
        byte[][] footerKeys,
        String[] aadPrefixFiles,
        byte[][] aadPrefixes
    ) {
        this(
            directoryPath,
            fileNamesToSegments(directoryPath, files),
            null,
            List.of(),
            List.of(),
            toMap(footerKeyFiles, footerKeys),
            toMap(aadPrefixFiles, aadPrefixes)
        );
    }

    private static List<MonoFileWriterSet> fileNamesToSegments(String directoryPath, String[] files) {
        if (files == null || files.length == 0) return List.of();
        return Arrays.stream(files)
            .map(f -> MonoFileWriterSet.of(directoryPath, 0L, f, 0L))
            .toList();
    }

    private static Map<String, byte[]> toMap(String[] keys, byte[][] values) {
        if (keys == null || keys.length == 0) return Map.of();
        Map<String, byte[]> map = new HashMap<>();
        IntStream.range(0, Math.min(keys.length, values.length))
            .forEach(i -> map.put(keys[i], values[i]));
        return map;
    }

    @Override
    protected void doClose() {
        if (ownsPointer) {
            NativeBridge.closeDatafusionReader(ptr);
        }
    }

    /**
     * Wraps a pre-existing native pointer without taking ownership (test only).
     * @param existingPtr the native pointer to wrap
     */
    public static ReaderHandle wrap(long existingPtr) {
        return new ReaderHandle(existingPtr);
    }
}
