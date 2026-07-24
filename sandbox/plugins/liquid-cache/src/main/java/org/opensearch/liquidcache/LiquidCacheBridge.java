/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.liquidcache;

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
 * FFM bridge binding the {@code lc_*} symbols in {@code libopensearch_native}.
 *
 * @opensearch.experimental
 */
public final class LiquidCacheBridge {

    private static final Logger logger = LogManager.getLogger(LiquidCacheBridge.class);

    public static final int STATS_LEN = 8;

    private static final MethodHandle LC_CREATE_OPTIMIZER;
    private static final MethodHandle LC_DESTROY_OPTIMIZER;
    private static final MethodHandle LC_SET_ENABLED;
    private static final MethodHandle LC_SET_MEMORY_LIMIT;
    private static final MethodHandle LC_SET_INDEXED_MAX_COLUMNS;
    private static final MethodHandle LC_SET_LISTING_MAX_COLUMNS;
    private static final MethodHandle LC_RESET_CACHE;
    private static final MethodHandle LC_STATS;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        // i64 lc_create_optimizer(i64 size, *const u8 policy_ptr, i64 policy_len, i64 enabled)
        LC_CREATE_OPTIMIZER = linker.downcallHandle(
            lib.find("lc_create_optimizer").orElseThrow(),
            FunctionDescriptor.of(
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG,
                ValueLayout.ADDRESS,
                ValueLayout.JAVA_LONG,
                ValueLayout.JAVA_LONG
            )
        );

        // void lc_destroy_optimizer(i64 handle)
        LC_DESTROY_OPTIMIZER = linker.downcallHandle(
            lib.find("lc_destroy_optimizer").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // void lc_set_enabled(i64 enabled)
        LC_SET_ENABLED = linker.downcallHandle(
            lib.find("lc_set_enabled").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // void lc_set_memory_limit(i64 bytes)
        LC_SET_MEMORY_LIMIT = linker.downcallHandle(
            lib.find("lc_set_memory_limit").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // void lc_set_indexed_max_columns(i64 count)
        LC_SET_INDEXED_MAX_COLUMNS = linker.downcallHandle(
            lib.find("lc_set_indexed_max_columns").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // void lc_set_listing_max_columns(i64 count)
        LC_SET_LISTING_MAX_COLUMNS = linker.downcallHandle(
            lib.find("lc_set_listing_max_columns").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
        );

        // void lc_reset_cache()
        LC_RESET_CACHE = linker.downcallHandle(
            lib.find("lc_reset_cache").orElseThrow(),
            FunctionDescriptor.ofVoid()
        );

        // void lc_stats(*mut i64 out)
        LC_STATS = linker.downcallHandle(
            lib.find("lc_stats").orElseThrow(),
            FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
        );

        logger.info("Liquid Cache FFM downcall handles resolved");
    }

    /** Initialize the global runtime and return a provider handle, or {@code 0} on failure. */
    public static long createOptimizer(long sizeBytes, String evictionPolicy, boolean enabled) {
        try (var call = new NativeCall()) {
            var policy = call.str(evictionPolicy);
            long handle = call.invoke(LC_CREATE_OPTIMIZER, sizeBytes, policy.segment(), policy.len(), enabled ? 1L : 0L);
            logger.info(
                "Liquid Cache optimizer created: sizeBytes={}, policy={}, enabled={}, handle=0x{}",
                sizeBytes,
                evictionPolicy,
                enabled,
                Long.toHexString(handle)
            );
            return handle;
        } catch (Exception e) {
            logger.error("lc_create_optimizer failed: {}", e.getMessage());
            return 0L;
        }
    }

    /** Free a handle from {@link #createOptimizer}. No-op for {@code 0}. */
    public static void destroyOptimizer(long handle) {
        if (handle == 0L) {
            return;
        }
        NativeCall.invokeVoid(LC_DESTROY_OPTIMIZER, handle);
        logger.info("Liquid Cache optimizer destroyed");
    }

    public static void setEnabled(boolean enabled) {
        NativeCall.invokeVoid(LC_SET_ENABLED, enabled ? 1L : 0L);
    }

    public static void setMemoryLimit(long bytes) {
        NativeCall.invokeVoid(LC_SET_MEMORY_LIMIT, bytes);
    }

    public static void setIndexedMaxColumns(long count) {
        NativeCall.invokeVoid(LC_SET_INDEXED_MAX_COLUMNS, count);
    }

    public static void setListingMaxColumns(long count) {
        NativeCall.invokeVoid(LC_SET_LISTING_MAX_COLUMNS, count);
    }

    public static void resetCache() {
        NativeCall.invokeVoid(LC_RESET_CACHE);
    }

    /**
     * Snapshot the {@link #STATS_LEN} counters, or zeros if uninitialized. Order:
     * {@code [cache_hit, cache_miss, predicate_evals, memory_evictions,
     * transcodes, total_entries, memory_usage_bytes, max_memory_bytes]}.
     */
    public static long[] stats() {
        try (Arena arena = Arena.ofConfined()) {
            var out = arena.allocate(ValueLayout.JAVA_LONG, STATS_LEN);
            LC_STATS.invokeExact(out);
            return out.toArray(ValueLayout.JAVA_LONG);
        } catch (Throwable t) {
            logger.warn("lc_stats failed: {}", t.getMessage());
            return new long[STATS_LEN];
        }
    }

    private LiquidCacheBridge() {}
}
