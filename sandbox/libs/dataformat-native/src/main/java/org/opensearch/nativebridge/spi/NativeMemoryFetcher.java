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
 * Stateless FFM fetcher for jemalloc memory metrics.
 * No caching, no server dependencies — just raw downcalls.
 */
public class NativeMemoryFetcher {

    private static final Logger logger = LogManager.getLogger(NativeMemoryFetcher.class);

    private static final MethodHandle ALLOCATED;
    private static final MethodHandle RESIDENT;

    static {
        SymbolLookup lookup = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        FunctionDescriptor desc = FunctionDescriptor.of(ValueLayout.JAVA_LONG);
        ALLOCATED = linker.downcallHandle(lookup.find("native_jemalloc_allocated_bytes").orElseThrow(), desc);
        RESIDENT = linker.downcallHandle(lookup.find("native_jemalloc_resident_bytes").orElseThrow(), desc);
    }

    private NativeMemoryFetcher() {}

    /**
     * Performs FFM downcalls and returns a fresh NativeMemoryStats snapshot.
     * Returns NativeMemoryStats(-1, -1) on error or negative values.
     */
    public static NativeMemoryStats fetch() {
        try {
            long allocated = (long) ALLOCATED.invokeExact();
            long resident = (long) RESIDENT.invokeExact();
            if (allocated < 0 || resident < 0) {
                logger.warn("Native memory stats returned error: allocated={}, resident={}", allocated, resident);
                return new NativeMemoryStats(-1, -1);
            }
            return new NativeMemoryStats(allocated, resident);
        } catch (Throwable t) {
            logger.warn("Error fetching native memory stats", t);
            return new NativeMemoryStats(-1, -1);
        }
    }
}
