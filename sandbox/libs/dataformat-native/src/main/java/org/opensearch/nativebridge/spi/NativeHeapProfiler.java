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

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * On-demand jemalloc heap profiling via FFM.
 * <p>
 * Provides methods to activate/deactivate heap profiling and dump heap profiles
 * at runtime. These are called by the NativeBridgeModule cluster settings listeners.
 * <p>
 * Requires jemalloc to be started with profiling support via environment variable:
 * {@code _RJEM_MALLOC_CONF="prof:true,prof_active:false,lg_prof_sample:17"}
 * <p>
 * <b>Security:</b> Heap dumps may contain sensitive in-memory data (index documents,
 * credentials, keys). Dump paths are restricted to the OpenSearch data directory.
 * The cluster setting requires operator-level privileges to modify.
 */
public final class NativeHeapProfiler {

    private static final Logger logger = LogManager.getLogger(NativeHeapProfiler.class);

    /** Allowed dump directory — null until explicitly set by createComponents(). */
    private static volatile String allowedDumpDir = null;

    private static final MethodHandle ACTIVATE;
    private static final MethodHandle DEACTIVATE;
    private static final MethodHandle DUMP;

    static {
        SymbolLookup lookup = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        FunctionDescriptor voidToLong = FunctionDescriptor.of(ValueLayout.JAVA_LONG);
        FunctionDescriptor ptrToLong = FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS);

        ACTIVATE = linker.downcallHandle(lookup.find("native_jemalloc_heap_prof_activate").orElseThrow(), voidToLong);
        DEACTIVATE = linker.downcallHandle(lookup.find("native_jemalloc_heap_prof_deactivate").orElseThrow(), voidToLong);
        DUMP = linker.downcallHandle(lookup.find("native_jemalloc_heap_prof_dump").orElseThrow(), ptrToLong);
    }

    private NativeHeapProfiler() {}

    /**
     * Sets the allowed directory for heap dumps. Called during module initialization
     * with the node's data path.
     */
    public static void setAllowedDumpDir(String dir) {
        allowedDumpDir = dir;
    }

    /**
     * Activates jemalloc heap profiling. Allocations will be sampled from this point.
     *
     * @param active true to activate, false to deactivate
     */
    public static void setActive(boolean active) {
        try {
            long rc;
            if (active) {
                rc = (long) ACTIVATE.invokeExact();
                NativeLibraryLoader.checkResult(rc);
                logger.info("jemalloc heap profiling activated");
            } else {
                rc = (long) DEACTIVATE.invokeExact();
                NativeLibraryLoader.checkResult(rc);
                logger.info("jemalloc heap profiling deactivated");
            }
        } catch (Throwable t) {
            logger.warn("Error toggling jemalloc heap profiling (is _RJEM_MALLOC_CONF=prof:true set?)", t);
        }
    }

    /**
     * Dumps a heap profile to the specified file path.
     * Path is validated to be within the allowed dump directory.
     *
     * @param path file path for the heap dump (e.g., "/tmp/heap_dump.heap")
     */
    public static void dumpProfile(String path) {
        if (path == null || path.isEmpty()) {
            return;
        }
        if (allowedDumpDir == null) {
            logger.warn("Heap dump rejected: module not yet initialized");
            return;
        }
        // Path validation: prevent arbitrary file writes
        java.nio.file.Path resolved;
        try {
            resolved = java.nio.file.Path.of(path).toAbsolutePath().normalize();
            java.nio.file.Path allowed = java.nio.file.Path.of(allowedDumpDir).toAbsolutePath().normalize();
            if (!resolved.startsWith(allowed)) {
                logger.warn("Heap dump path [{}] rejected: must be under [{}]", path, allowedDumpDir);
                return;
            }
            path = resolved.toString();
        } catch (Exception e) {
            logger.warn("Invalid heap dump path [{}]: {}", path, e.getMessage());
            return;
        }

        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cPath = arena.allocateFrom(path);
            long rc = (long) DUMP.invokeExact(cPath);
            NativeLibraryLoader.checkResult(rc);
            // Restrict file permissions to owner-only (600)
            try {
                java.nio.file.Files.setPosixFilePermissions(resolved,
                    java.util.Set.of(
                        java.nio.file.attribute.PosixFilePermission.OWNER_READ,
                        java.nio.file.attribute.PosixFilePermission.OWNER_WRITE
                    ));
            } catch (Exception e) {
                logger.debug("Could not set file permissions on heap dump (non-POSIX filesystem?): {}", e.getMessage());
            }
            logger.info("jemalloc heap profile dumped to {} (permissions: 600)", path);
        } catch (Throwable t) {
            logger.warn("Error dumping jemalloc heap profile to " + path, t);
        }
    }
}
