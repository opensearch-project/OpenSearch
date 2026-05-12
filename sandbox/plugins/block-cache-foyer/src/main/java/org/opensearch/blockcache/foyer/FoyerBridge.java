/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.foyer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.nativebridge.spi.NativeCall;
import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * FFM bridge for the Foyer block cache lifecycle.
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
 * exactly once when the cache is no  longer needed.
 *
 * @opensearch.experimental
 */
public final class FoyerBridge {

    private static final Logger logger = LogManager.getLogger(FoyerBridge.class);

    private static final MethodHandle FOYER_CREATE_CACHE;
    private static final MethodHandle FOYER_DESTROY_CACHE;
    private static final MethodHandle FOYER_SNAPSHOT_STATS;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        // i64 foyer_create_cache(u64 disk_bytes, *const u8 dir_ptr, u64 dir_len,
        // u64 block_size_bytes,
        // *const u8 io_engine_ptr, u64 io_engine_len)
        FOYER_CREATE_CACHE = linker.downcallHandle(
            lib.find("foyer_create_cache").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,  // return: opaque i64 handle
                ValueLayout.JAVA_LONG,  // disk_bytes: u64
                ValueLayout.ADDRESS,    // dir_ptr: *const u8
                ValueLayout.JAVA_LONG,  // dir_len: u64
                ValueLayout.JAVA_LONG,  // block_size_bytes: u64
                ValueLayout.ADDRESS,    // io_engine_ptr: *const u8
                ValueLayout.JAVA_LONG   // io_engine_len: u64
            )
        );

        // i64 foyer_destroy_cache(i64 ptr) — 0=success, <0=error pointer
        FOYER_DESTROY_CACHE = linker.downcallHandle(
            lib.find("foyer_destroy_cache").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,  // return: 0=ok, <0=error
                ValueLayout.JAVA_LONG   // ptr
            )
        );

        // i64 foyer_snapshot_stats(i64 ptr, i64* out) — 0=success, <0=error
        // Writes BlockCacheStats.Field.COUNT * 2 i64 values into out:
        // overall section then block-level section.
        // See BlockCacheStats.Field for the per-section field layout.
        FOYER_SNAPSHOT_STATS = linker.downcallHandle(
            lib.find("foyer_snapshot_stats").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,  // return: 0=ok, <0=error
                ValueLayout.JAVA_LONG,  // ptr: i64 cache handle
                ValueLayout.ADDRESS     // out: *mut i64, output buffer
            )
        );

        logger.info("FFM downcall handles resolved: foyer_create_cache, foyer_destroy_cache, foyer_snapshot_stats");
    }

    /**
     * Create a Foyer block cache.
     *
     * @param diskBytes       maximum disk space the cache may use, in bytes
     * @param diskDir         path to the directory where Foyer stores cache data
     * @param blockSizeBytes  Foyer disk block size in bytes (see {@code format_cache.block_size})
     * @param ioEngine        I/O engine: {@code "auto"}, {@code "io_uring"}, or {@code "psync"}
     *                        (see {@code format_cache.io_engine})
     * @return an opaque handle representing the cache instance; always positive on success
     * @throws RuntimeException if the native call fails or the directory is invalid
     */
    public static long createCache(long diskBytes, String diskDir, long blockSizeBytes, String ioEngine) {
        try (var call = new NativeCall()) {
            var dir = call.str(diskDir);
            var engine = call.str(ioEngine);
            long ptr = call.invoke(FOYER_CREATE_CACHE, diskBytes, dir.segment(), dir.len(), blockSizeBytes, engine.segment(), engine.len());
            if (ptr <= 0) {
                throw new IllegalStateException("foyer_create_cache returned an invalid handle");
            }
            logger.info(
                "Foyer block cache created: diskBytes={}, blockSizeBytes={}, ioEngine={}, dir={}",
                diskBytes,
                blockSizeBytes,
                ioEngine,
                diskDir
            );
            return ptr;
        }
    }

    /**
     * Destroy a cache previously created by {@link #createCache}.
     *
     * <p>After this call the handle is invalid and must not be used again.
     *
     * @param ptr the handle returned by {@link #createCache}
     * @throws RuntimeException if the native call returns an error (invalid ptr)
     */
    public static void destroyCache(long ptr) {
        try (var call = new NativeCall()) {
            call.invoke(FOYER_DESTROY_CACHE, ptr);
        }
        logger.info("Foyer block cache destroyed");
    }

    /**
     * Snapshot the cache statistics from the native Foyer runtime.
     *
     * <p>Returns a {@code long[14]} buffer containing two equal-sized sections:
     * {@code overall} (cross-tier rollup, indices 0–6) followed by
     * {@code block_level} (disk tier, indices 7–13).
     * Each section carries the 7 counters in the order defined by
     * {@code FoyerAggregatedStats.Field}: HIT_COUNT, HIT_BYTES, MISS_COUNT,
     * MISS_BYTES, EVICTION_COUNT, EVICTION_BYTES, USED_BYTES.
     *
     * <p>The buffer size (14 = 7 fields × 2 sections) must stay in sync with
     * the Rust {@code foyer_snapshot_stats} implementation.
     */
    public static long[] snapshotStats(long ptr) {
        // 7 counters per section (must match FoyerAggregatedStats.Field ordinals)
        // × 2 sections (overall + block_level) = 14 longs total.
        final int bufferSize = 14;
        try (Arena arena = Arena.ofConfined()) {
            // Arena.allocate(ValueLayout, count) is the Java 22+ replacement for
            // the removed Arena.allocateArray(ValueLayout, count).
            var out = arena.allocate(ValueLayout.JAVA_LONG, bufferSize);
            try (var call = new NativeCall()) {
                call.invoke(FOYER_SNAPSHOT_STATS, ptr, out);
            }
            // toArray copies the entire allocated segment.
            long[] result = out.toArray(ValueLayout.JAVA_LONG);
            if (result.length != bufferSize) {
                // Native/Java buffer size mismatch — log and return zeros rather than
                // silently mismap fields, which would corrupt stats.
                logger.error(
                    "foyer_snapshot_stats: expected {} values but got {}; "
                    + "Rust snapshot() and Java FoyerAggregatedStats.Field are out of sync",
                    bufferSize, result.length
                );
                return new long[bufferSize];
            }
            return result;
        } catch (Exception e) {
            logger.warn("foyer_snapshot_stats failed: {}", e.getMessage());
            return new long[bufferSize];
        }
    }

    private FoyerBridge() {}
}
