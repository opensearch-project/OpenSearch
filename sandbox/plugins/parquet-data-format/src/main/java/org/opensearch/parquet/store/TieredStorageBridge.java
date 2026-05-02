/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.store;

import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bridge for tiered storage Rust functions.
 *
 * <p>Methods: create/destroy TieredObjectStore, batch register files, remove file.
 *
 * <p>The {@code registerFiles} method uses a newline-delimited batch format:
 * {@code path\nremotePath\npath\nremotePath\n...} Empty remotePath for LOCAL files.
 * This avoids per-file FFM overhead when seeding hundreds of files at shard open.
 */
public final class TieredStorageBridge {

    private static final MethodHandle CREATE;
    private static final MethodHandle DESTROY;
    private static final MethodHandle REGISTER_FILES;
    private static final MethodHandle REMOVE_FILE;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        CREATE = linker.downcallHandle(
            lib.find("ts_create_tiered_object_store").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        DESTROY = linker.downcallHandle(
            lib.find("ts_destroy_tiered_object_store").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        REGISTER_FILES = linker.downcallHandle(
            lib.find("ts_register_files").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_INT,
                ValueLayout.JAVA_INT
            )
        );
        REMOVE_FILE = linker.downcallHandle(
            lib.find("ts_remove_file").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
    }

    private TieredStorageBridge() {}

    /**
     * Create a TieredObjectStore with optional local and remote stores.
     *
     * @param localStorePtr  Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer, or 0 for default LocalFileSystem
     * @param remoteStorePtr Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer, or 0 for no remote
     * @return native pointer to the TieredObjectStore
     */
    public static long createTieredObjectStore(long localStorePtr, long remoteStorePtr) {
        try {
            return NativeLibraryLoader.checkResult((long) CREATE.invokeExact(localStorePtr, remoteStorePtr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create TieredObjectStore", t);
        }
    }

    /** Destroy a TieredObjectStore and its internal registry. */
    public static void destroyTieredObjectStore(long ptr) {
        try {
            NativeLibraryLoader.checkResult((long) DESTROY.invokeExact(ptr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to destroy TieredObjectStore", t);
        }
    }

    /**
     * Register files in the registry. Batch format: triplets of path\nremotePath\nsize\n...
     * location: 0=Local, 1=Remote — applied to all files in the batch.
     *
     * @param storePtr   native pointer to the TieredObjectStore
     * @param fileToPath map of file path to remote path (remote path can be empty for Local)
     * @param location   0=Local, 1=Remote
     * @param size       file size in bytes (applied to all files in batch)
     */
    public static void registerFiles(long storePtr, java.util.Map<String, String> fileToPath, int location, long size) {
        if (fileToPath.isEmpty()) return;
        StringBuilder sb = new StringBuilder();
        for (java.util.Map.Entry<String, String> e : fileToPath.entrySet()) {
            sb.append(e.getKey()).append('\n');
            sb.append(e.getValue() != null ? e.getValue() : "").append('\n');
            sb.append(size).append('\n');
        }
        sb.setLength(sb.length() - 1);
        String entries = sb.toString();
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg = arena.allocateFrom(entries);
            NativeLibraryLoader.checkResult(
                (long) REGISTER_FILES.invokeExact(storePtr, seg, (long) entries.length(), fileToPath.size(), location)
            );
        } catch (Throwable t) {
            throw new RuntimeException("Failed to register " + fileToPath.size() + " files", t);
        }
    }

    /**
     * Register a single file in the registry with its own location and size.
     *
     * @param storePtr native pointer to the TieredObjectStore
     * @param file     file identifier (absolute path for DataFusion lookups)
     * @param path     blob path (remote path for REMOTE, local path for LOCAL)
     * @param location 0=Local, 1=Remote
     * @param size     file size in bytes
     */
    public static void registerFile(long storePtr, String file, String path, int location, long size) {
        registerFiles(storePtr, java.util.Map.of(file, path != null ? path : ""), location, size);
    }

    /** Remove a file from the registry. */
    public static void removeFile(long storePtr, String path) {
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg = arena.allocateFrom(path);
            NativeLibraryLoader.checkResult((long) REMOVE_FILE.invokeExact(storePtr, seg, (long) path.length()));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to remove file: " + path, t);
        }
    }
}
