/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache.foyer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bridge for the Foyer page cache lifecycle.
 *
 * <p>Exposes two operations: {@link #createCache} and {@link #destroyCache}.
 * These map to the {@code foyer_create_cache} and {@code foyer_destroy_cache}
 * symbols exported by the native library.
 *
 * <p>Cache access operations ({@code get}, {@code put}, {@code evict}) are not
 * exposed here — they are called directly from the native layer without
 * crossing the Java boundary.
 *
 * <p>{@link #createCache} returns an opaque {@code long} handle that represents
 * the native cache instance. The handle must be passed to {@link #destroyCache}
 * exactly once when the cache is no longer needed.
 *
 * @opensearch.experimental
 */
public final class FoyerBridge {

    private static final Logger logger = LogManager.getLogger(FoyerBridge.class);

    private static final MethodHandle FOYER_CREATE_CACHE;
    private static final MethodHandle FOYER_DESTROY_CACHE;

    static {
        SymbolLookup lib    = NativeLibraryLoader.symbolLookup();
        Linker       linker = Linker.nativeLinker();

        // i64 foyer_create_cache(u64 disk_bytes, *const u8 dir_ptr, u64 dir_len)
        FOYER_CREATE_CACHE = linker.downcallHandle(
            lib.find("foyer_create_cache").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,  // return: opaque i64 handle
                ValueLayout.JAVA_LONG,  // disk_bytes: u64
                ValueLayout.ADDRESS,    // dir_ptr: *const u8
                ValueLayout.JAVA_LONG   // dir_len: u64
            )
        );

        // void foyer_destroy_cache(i64 ptr)
        FOYER_DESTROY_CACHE = linker.downcallHandle(
            lib.find("foyer_destroy_cache").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );
        logger.info("[FoyerBridge] FFM handles resolved");
    }

    /**
     * Create a Foyer cache with the given disk capacity and storage directory.
     *
     * @param diskBytes maximum disk space the cache may use, in bytes
     * @param diskDir   path to the directory where Foyer stores cache data
     * @return an opaque handle representing the cache instance; always positive on success
     * @throws RuntimeException if the native call fails or the directory is invalid
     */
    public static long createCache(long diskBytes, String diskDir) {
        try (var call = new NativeCall()) {
            var dir = call.str(diskDir);
            long ptr = call.invoke(FOYER_CREATE_CACHE, diskBytes, dir.segment(), dir.len());
            logger.info("[FoyerBridge] cache created: ptr={}, diskBytes={}, dir={}", ptr, diskBytes, diskDir);
            return ptr;
        }
    }

    /**
     * Destroy a cache previously created by {@link #createCache}.
     *
     * <p>After this call the handle is invalid and must not be used again.
     *
     * @param ptr the handle returned by {@link #createCache}
     */
    public static void destroyCache(long ptr) {
        NativeCall.invokeVoid(FOYER_DESTROY_CACHE, ptr);
        logger.info("[FoyerBridge] cache destroyed: ptr={}", ptr);
    }

    private FoyerBridge() {}
}
