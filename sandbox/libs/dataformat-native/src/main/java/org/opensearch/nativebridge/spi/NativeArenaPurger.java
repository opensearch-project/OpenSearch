/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Periodic jemalloc arena purger with threshold-based activation.
 * <p>
 * Delegates to a dedicated Rust OS thread ({@code jemalloc-purge}) that periodically
 * checks jemalloc resident bytes and purges all arenas if resident exceeds the
 * configured threshold. The Rust thread reads jemalloc stats and calls mallctl
 * directly — no FFI round-trip per check cycle.
 *
 * <p>Java controls the thread via FFI setters for threshold and interval. The thread
 * is started once at node startup and runs until process exit.
 *
 * <p><b>Configuration:</b>
 * <ul>
 *   <li>{@code native.jemalloc.purge_threshold_percent} — purge fires only when
 *       resident exceeds this percentage of {@code node.native_memory.limit}. Default: 85%.</li>
 *   <li>{@code native.jemalloc.purge_interval} — how often to check. Default: 5s.</li>
 * </ul>
 */
public final class NativeArenaPurger {

    private static final Logger logger = LogManager.getLogger(NativeArenaPurger.class);

    private static final MethodHandle START_PURGE_THREAD;
    private static final MethodHandle STOP_PURGE_THREAD;
    private static final MethodHandle SET_THRESHOLD;
    private static final MethodHandle SET_INTERVAL;
    private static final MethodHandle GET_PURGE_COUNT;

    static {
        SymbolLookup lookup = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        FunctionDescriptor voidToLong = FunctionDescriptor.of(ValueLayout.JAVA_LONG);
        FunctionDescriptor longToLong = FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);
        START_PURGE_THREAD = linker.downcallHandle(
            lookup.find("native_jemalloc_start_purge_thread").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        STOP_PURGE_THREAD = linker.downcallHandle(lookup.find("native_jemalloc_stop_purge_thread").orElseThrow(), voidToLong);
        SET_THRESHOLD = linker.downcallHandle(lookup.find("native_jemalloc_set_purge_threshold").orElseThrow(), longToLong);
        SET_INTERVAL = linker.downcallHandle(lookup.find("native_jemalloc_set_purge_interval").orElseThrow(), longToLong);
        GET_PURGE_COUNT = linker.downcallHandle(lookup.find("native_jemalloc_get_purge_count").orElseThrow(), voidToLong);
    }

    private NativeArenaPurger() {}

    /**
     * Starts the Rust-side background purge thread. Idempotent — only the first call
     * spawns the thread. Must be called once at node startup.
     */
    public static void init(long thresholdBytes, long intervalMs) {
        try {
            long rc = (long) START_PURGE_THREAD.invokeExact(thresholdBytes, intervalMs);
            if (rc != 0) {
                logger.warn("native_jemalloc_start_purge_thread returned {}", rc);
            }
            logger.info("jemalloc purge thread started (threshold={} MB, interval={} ms)", thresholdBytes / (1024 * 1024), intervalMs);
        } catch (Throwable t) {
            logger.warn("Failed to start jemalloc purge thread", t);
        }
    }

    /**
     * Sets the threshold in absolute bytes. Purge fires only when resident exceeds this.
     * Typically computed as {@code node.native_memory.limit * purge_threshold_percent / 100}.
     */
    public static void setThresholdBytes(long bytes) {
        try {
            long rc = (long) SET_THRESHOLD.invokeExact(bytes);
            if (rc != 0) {
                logger.warn("native_jemalloc_set_purge_threshold returned {}", rc);
            }
            logger.info("jemalloc purge threshold updated to {} MB", bytes / (1024 * 1024));
        } catch (Throwable t) {
            logger.warn("Failed to set purge threshold", t);
        }
    }

    /**
     * Sets the check interval in milliseconds. Set to 0 to pause periodic purging.
     */
    public static void setCheckIntervalMs(long ms) {
        try {
            long rc = (long) SET_INTERVAL.invokeExact(ms);
            if (rc != 0) {
                logger.warn("native_jemalloc_set_purge_interval returned {}", rc);
            }
            logger.info("jemalloc purge check interval updated to {} ms", ms);
        } catch (Throwable t) {
            logger.warn("Failed to set purge interval", t);
        }
    }

    /** Number of times a purge was actually executed by the Rust background thread. */
    public static long getPurgeCount() {
        try {
            return (long) GET_PURGE_COUNT.invokeExact();
        } catch (Throwable t) {
            return 0;
        }
    }

    /** Stops the Rust-side purge thread. Called during node shutdown. */
    public static void stop() {
        try {
            long rc = (long) STOP_PURGE_THREAD.invokeExact();
            if (rc != 0) {
                logger.warn("native_jemalloc_stop_purge_thread returned {}", rc);
            }
        } catch (Throwable t) {
            logger.warn("Failed to stop purge thread", t);
        }
    }

}
