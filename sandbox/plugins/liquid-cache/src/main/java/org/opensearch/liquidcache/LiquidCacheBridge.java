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
 * FFM bridge for liquid cache, which is compiled into the shared analytics engine
 * native library when the engine is built with the {@code liquid_cache} cargo
 * feature (Gradle {@code -PliquidCache}). The {@code lc_*} symbols are bound from
 * that engine library; there is no separate provider library to load.
 *
 * <p>When the engine was built without the feature, the {@code lc_*} symbols are
 * absent, {@link #isAvailable()} is {@code false}, and every call here is a no-op —
 * the plugin loads inert and nothing fails initialization.
 *
 * @opensearch.experimental
 */
public final class LiquidCacheBridge {

    private static final Logger logger = LogManager.getLogger(LiquidCacheBridge.class);

    public static final int STATS_LEN = 8;

    private static final boolean AVAILABLE;

    // All bound from the shared engine library (present only when built with the
    // liquid_cache cargo feature).
    private static final MethodHandle LC_INIT;
    private static final MethodHandle LC_SET_ENABLED;
    private static final MethodHandle LC_SET_MEMORY_LIMIT;
    private static final MethodHandle LC_SET_INDEXED_MAX_COLUMNS;
    private static final MethodHandle LC_SET_LISTING_MAX_COLUMNS;
    private static final MethodHandle LC_RESET_CACHE;
    private static final MethodHandle LC_STATS;

    static {
        Linker linker = Linker.nativeLinker();
        MethodHandle init = null, setEnabled = null, setMem = null, setIdx = null, setList = null, reset = null,
            stats = null;
        boolean available = false;
        try {
            SymbolLookup engine = NativeLibraryLoader.symbolLookup();
            // lc_init(max_memory_bytes i64, enabled i64, eviction_ptr, eviction_len i64) -> i64
            var initSym = engine.find("lc_init");
            if (initSym.isPresent()) {
                FunctionDescriptor voidLong = FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG);
                init = linker.downcallHandle(
                    initSym.get(),
                    FunctionDescriptor.of(
                        ValueLayout.JAVA_LONG,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.JAVA_LONG,
                        ValueLayout.ADDRESS,
                        ValueLayout.JAVA_LONG
                    )
                );
                setEnabled = linker.downcallHandle(engine.find("lc_set_enabled").orElseThrow(), voidLong);
                setMem = linker.downcallHandle(engine.find("lc_set_memory_limit").orElseThrow(), voidLong);
                setIdx = linker.downcallHandle(engine.find("lc_set_indexed_max_columns").orElseThrow(), voidLong);
                setList = linker.downcallHandle(engine.find("lc_set_listing_max_columns").orElseThrow(), voidLong);
                reset = linker.downcallHandle(engine.find("lc_reset_cache").orElseThrow(), FunctionDescriptor.ofVoid());
                stats = linker.downcallHandle(
                    engine.find("lc_stats").orElseThrow(),
                    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
                );
                available = true;
                logger.info("Liquid Cache bound from the engine native library (liquid_cache feature enabled)");
            } else {
                logger.warn("Liquid Cache disabled: engine native library was built without the liquid_cache feature");
            }
        } catch (Throwable t) {
            logger.error("Liquid Cache bridge init failed: {}", t.toString());
            available = false;
        }

        LC_INIT = init;
        LC_SET_ENABLED = setEnabled;
        LC_SET_MEMORY_LIMIT = setMem;
        LC_SET_INDEXED_MAX_COLUMNS = setIdx;
        LC_SET_LISTING_MAX_COLUMNS = setList;
        LC_RESET_CACHE = reset;
        LC_STATS = stats;
        AVAILABLE = available;
    }

    /** True when the engine was built with the liquid cache feature and the symbols are bound. */
    public static boolean isAvailable() {
        return AVAILABLE;
    }

    /** Build the process-global cache. Returns true on success. Idempotent (first call wins). */
    public static boolean init(long maxMemoryBytes, boolean enabled, String evictionPolicy) {
        if (LC_INIT == null) {
            return false;
        }
        try (NativeCall call = new NativeCall()) {
            var e = call.str(evictionPolicy == null || evictionPolicy.isBlank() ? "lru" : evictionPolicy);
            long rc = call.invoke(LC_INIT, maxMemoryBytes, enabled ? 1L : 0L, e.segment(), e.len());
            if (rc != 0L) {
                logger.warn("lc_init returned {}", rc);
                return false;
            }
            logger.info("liquid cache initialized: max_memory={}B, enabled={}", maxMemoryBytes, enabled);
            return true;
        } catch (Exception ex) {
            logger.error("lc_init failed: {}", ex.getMessage());
            return false;
        }
    }

    public static void setEnabled(boolean enabled) {
        if (LC_SET_ENABLED != null) {
            NativeCall.invokeVoid(LC_SET_ENABLED, enabled ? 1L : 0L);
        }
    }

    public static void setMemoryLimit(long bytes) {
        if (LC_SET_MEMORY_LIMIT != null) {
            NativeCall.invokeVoid(LC_SET_MEMORY_LIMIT, bytes);
        }
    }

    public static void setIndexedMaxColumns(long count) {
        if (LC_SET_INDEXED_MAX_COLUMNS != null) {
            NativeCall.invokeVoid(LC_SET_INDEXED_MAX_COLUMNS, count);
        }
    }

    public static void setListingMaxColumns(long count) {
        if (LC_SET_LISTING_MAX_COLUMNS != null) {
            NativeCall.invokeVoid(LC_SET_LISTING_MAX_COLUMNS, count);
        }
    }

    public static void resetCache() {
        if (LC_RESET_CACHE != null) {
            NativeCall.invokeVoid(LC_RESET_CACHE);
        }
    }

    /**
     * Snapshot the {@link #STATS_LEN} counters, or zeros if unavailable. Order:
     * {@code [cache_hit, cache_miss, predicate_evals, memory_evictions,
     * transcodes, total_entries, memory_usage_bytes, max_memory_bytes]}.
     */
    public static long[] stats() {
        if (LC_STATS == null) {
            return new long[STATS_LEN];
        }
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
