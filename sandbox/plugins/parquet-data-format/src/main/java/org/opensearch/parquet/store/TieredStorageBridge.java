/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.store;

import org.opensearch.index.engine.dataformat.FileResolver;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FFM bridge for tiered storage Rust functions.
 *
 * <p>Methods: create/destroy TieredObjectStore, batch register files, remove file,
 * block-cache pointer helpers, and lazy file resolution via upcall.
 *
 * <p>The lazy resolver enables Rust to call back into Java when a file is not
 * found in the native registry at query time. Resolvers are registered per
 * store pointer via {@link #registerResolver(long, FileResolver)} and
 * unregistered via {@link #unregisterResolver(long)}.
 */
public final class TieredStorageBridge {

    private static final MethodHandle CREATE;
    private static final MethodHandle DESTROY;
    private static final MethodHandle REGISTER_FILES;
    private static final MethodHandle REMOVE_FILE;
    private static final MethodHandle GET_OBJECT_STORE_BOX_PTR;
    private static final MethodHandle DESTROY_OBJECT_STORE_BOX_PTR;

    /** Global map: native store pointer → file resolver for that shard. */
    private static final ConcurrentHashMap<Long, FileResolver> RESOLVER_MAP = new ConcurrentHashMap<>();

    /** Upcall stub for lazy file resolution. Created once, JVM lifetime. */
    private static final MemorySegment RESOLVE_UPCALL_STUB;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        // Create the upcall stub for lazy resolution
        try {
            MethodHandle resolveHandle = MethodHandles.lookup()
                .findStatic(
                    TieredStorageBridge.class,
                    "lazyResolveFromRust",
                    MethodType.methodType(long.class, long.class, MemorySegment.class, long.class)
                );
            RESOLVE_UPCALL_STUB = linker.upcallStub(
                resolveHandle,
                FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG),
                Arena.global()
            );
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new ExceptionInInitializerError(e);
        }

        // i64 ts_create_tiered_object_store(i64 local, i64 remote, i64 cache, i64 resolve_cb)
        CREATE = linker.downcallHandle(
            lib.find("ts_create_tiered_object_store").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,   // return: Arc<TieredObjectStore> ptr
                ValueLayout.JAVA_LONG,   // local_store_box_ptr (0 = default LocalFileSystem)
                ValueLayout.JAVA_LONG,   // remote_store_box_ptr (0 = no remote)
                ValueLayout.JAVA_LONG,   // cache_box_ptr (0 = no cache)
                ValueLayout.JAVA_LONG    // resolve_cb_ptr (0 = no lazy resolver)
            )
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
        GET_OBJECT_STORE_BOX_PTR = linker.downcallHandle(
            lib.find("ts_get_object_store_box_ptr").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        // Optional — graceful if native library is stale and symbol not yet available.
        DESTROY_OBJECT_STORE_BOX_PTR = lib.find("ts_destroy_object_store_box_ptr")
            .map(sym -> linker.downcallHandle(sym, FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)))
            .orElse(null);

    }

    private TieredStorageBridge() {}

    /**
     * Create a TieredObjectStore with optional local store, remote store, and block cache.
     * No lazy resolver — for backward compatibility.
     *
     * @param localStorePtr  Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer, or 0 for default LocalFileSystem
     * @param remoteStorePtr Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer, or 0 for no remote
     * @param cacheBoxPtr    block cache pointer, or 0 for no cache
     * @return native Arc&lt;TieredObjectStore&gt; pointer
     */
    public static long createTieredObjectStore(long localStorePtr, long remoteStorePtr, long cacheBoxPtr) {
        try {
            return NativeLibraryLoader.checkResult((long) CREATE.invokeExact(localStorePtr, remoteStorePtr, cacheBoxPtr, 0L));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create TieredObjectStore", t);
        }
    }

    /**
     * Create a TieredObjectStore with optional local store, remote store, block cache,
     * and a lazy file resolver upcall stub.
     *
     * @param localStorePtr      Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer, or 0 for default LocalFileSystem
     * @param remoteStorePtr     Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer, or 0 for no remote
     * @param cacheBoxPtr        block cache pointer, or 0 for no cache
     * @param resolveStub        upcall stub for lazy file resolution
     * @return native Arc&lt;TieredObjectStore&gt; pointer
     */
    public static long createTieredObjectStoreWithResolver(
        long localStorePtr,
        long remoteStorePtr,
        long cacheBoxPtr,
        java.lang.foreign.MemorySegment resolveStub
    ) {
        try {
            long resolveAddr = (resolveStub != null) ? resolveStub.address() : 0L;
            return NativeLibraryLoader.checkResult((long) CREATE.invokeExact(localStorePtr, remoteStorePtr, cacheBoxPtr, resolveAddr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create TieredObjectStore with resolver", t);
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

    /**
     * Get a Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer from a TieredObjectStore Arc pointer.
     * This is the format that DataFusion's df_create_reader expects.
     * The returned pointer shares ownership with the original — free it with destroyObjectStoreBoxPtr.
     *
     * @param tieredStorePtr the Arc&lt;TieredObjectStore&gt; pointer from createTieredObjectStore
     * @return Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer for DataFusion
     */
    public static long getObjectStoreBoxPtr(long tieredStorePtr) {
        try {
            return NativeLibraryLoader.checkResult((long) GET_OBJECT_STORE_BOX_PTR.invokeExact(tieredStorePtr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get object store box ptr", t);
        }
    }

    /**
     * Free a Box&lt;Arc&lt;dyn ObjectStore&gt;&gt; pointer returned by getObjectStoreBoxPtr.
     * Drops the Box and decrements the Arc strong count.
     * No-op if the native symbol is not available (stale library).
     */
    public static void destroyObjectStoreBoxPtr(long boxPtr) {
        if (boxPtr <= 0) return;
        if (DESTROY_OBJECT_STORE_BOX_PTR == null) return;
        try {
            NativeLibraryLoader.checkResult((long) DESTROY_OBJECT_STORE_BOX_PTR.invokeExact(boxPtr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to destroy object store box ptr", t);
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // Lazy file resolver — upcall from Rust on registry miss
    // ═══════════════════════════════════════════════════════════════

    /**
     * Register a file resolver for a store. Called by StoreStrategyRegistry at shard open.
     * The resolver is invoked from Rust (via upcall) when a file is not in the native registry.
     *
     * @param storePtr the native TieredObjectStore pointer
     * @param resolver the resolver that can look up remote paths for this shard's files
     */
    public static void registerResolver(long storePtr, FileResolver resolver) {
        RESOLVER_MAP.put(storePtr, resolver);
    }

    /**
     * Unregister a file resolver. Called at shard close before destroying the native store.
     *
     * @param storePtr the native TieredObjectStore pointer
     */
    public static void unregisterResolver(long storePtr) {
        RESOLVER_MAP.remove(storePtr);
    }

    /**
     * Returns the upcall stub address for passing to Rust at store creation time.
     * The stub has signature: {@code (path_ptr: ADDRESS, path_len: LONG) -> LONG}.
     */
    public static MemorySegment getResolveUpcallStub() {
        return RESOLVE_UPCALL_STUB;
    }

    /**
     * Static upcall target called from Rust ({@code TieredObjectStore.try_lazy_resolve()})
     * when a file is not in the native registry at query time.
     *
     * <p>Uses the store pointer for O(1) lookup in {@link #RESOLVER_MAP} to find
     * the correct shard's resolver.
     *
     * @param storePtr the native TieredObjectStore pointer (identifies the shard)
     * @param pathPtr pointer to UTF-8 bytes of the absolute file path (unbounded native segment)
     * @param pathLen length of the path in bytes
     * @return 1 if the file was resolved and registered, 0 on failure
     */
    public static long lazyResolveFromRust(long storePtr, MemorySegment pathPtr, long pathLen) {
        try {
            FileResolver resolver = RESOLVER_MAP.get(storePtr);
            if (resolver == null) return 0;

            byte[] bytes = pathPtr.reinterpret(pathLen).toArray(ValueLayout.JAVA_BYTE);
            String absoluteKey = new String(bytes, StandardCharsets.UTF_8);

            org.opensearch.index.engine.dataformat.DataFormatStoreHandler.FileEntry result = resolver.resolve(absoluteKey);
            if (result == null) return 0;

            registerFile(storePtr, absoluteKey, result.path(), result.location(), result.size());
            return 1;
        } catch (Exception e) {
            return 0;
        }
    }
}
