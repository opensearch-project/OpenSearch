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
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * JMX MBean for on-demand jemalloc heap profiling.
 * <p>
 * Registered as {@code org.opensearch.native:type=HeapProfiler} in the platform MBean server.
 * The CLI tool {@code bin/opensearch-heap-prof} connects via JMX Attach API to invoke these
 * operations — no REST API or cluster settings required.
 * <p>
 * This is the native equivalent of using jcmd for JVM heap dumps.
 */
public class NativeHeapProfiler implements NativeHeapProfilerMBean {

    private static final Logger logger = LogManager.getLogger(NativeHeapProfiler.class);
    public static final String MBEAN_NAME = "org.opensearch.native:type=HeapProfiler";

    private static final MethodHandle ACTIVATE;
    private static final MethodHandle DEACTIVATE;
    private static final MethodHandle DUMP;
    private static final MethodHandle RESET;

    static {
        SymbolLookup lookup = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();

        FunctionDescriptor voidToLong = FunctionDescriptor.of(ValueLayout.JAVA_LONG);
        FunctionDescriptor ptrToLong = FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS);
        FunctionDescriptor longToLong = FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG);

        ACTIVATE = linker.downcallHandle(lookup.find("native_jemalloc_heap_prof_activate").orElseThrow(), voidToLong);
        DEACTIVATE = linker.downcallHandle(lookup.find("native_jemalloc_heap_prof_deactivate").orElseThrow(), voidToLong);
        DUMP = linker.downcallHandle(lookup.find("native_jemalloc_heap_prof_dump").orElseThrow(), ptrToLong);
        RESET = linker.downcallHandle(lookup.find("native_jemalloc_heap_prof_reset").orElseThrow(), longToLong);
    }

    private volatile boolean active = false;

    /**
     * Registers this MBean in the platform MBean server.
     * Called once during NativeBridgeModule initialization.
     */
    public static void register() {
        try {
            MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            ObjectName name = new ObjectName(MBEAN_NAME);
            if (!mbs.isRegistered(name)) {
                mbs.registerMBean(new NativeHeapProfiler(), name);
                logger.info("Native heap profiler MBean registered: {}", MBEAN_NAME);
            }
        } catch (Exception e) {
            logger.warn("Failed to register native heap profiler MBean", e);
        }
    }

    @Override
    public void activate() {
        try {
            long rc = (long) ACTIVATE.invokeExact();
            NativeLibraryLoader.checkResult(rc);
            active = true;
            logger.info("jemalloc heap profiling activated");
        } catch (Throwable t) {
            throw new RuntimeException("Failed to activate heap profiling", t);
        }
    }

    @Override
    public void deactivate() {
        try {
            long rc = (long) DEACTIVATE.invokeExact();
            NativeLibraryLoader.checkResult(rc);
            active = false;
            logger.info("jemalloc heap profiling deactivated");
        } catch (Throwable t) {
            throw new RuntimeException("Failed to deactivate heap profiling", t);
        }
    }

    /** Allowed dump directories. Defaults to data dir only. Configurable via system property. */
    private static volatile java.util.List<String> allowedDumpDirs = java.util.List.of();

    /**
     * Sets the allowed directories for heap dumps. Called during module initialization.
     */
    public static void setAllowedDumpDirs(java.util.List<String> dirs) {
        allowedDumpDirs = java.util.List.copyOf(dirs);
    }

    @Override
    public String dump(String path) {
        if (path == null || path.isEmpty()) {
            throw new IllegalArgumentException("Dump path must not be empty");
        }
        if (path.contains("..")) {
            throw new IllegalArgumentException("Path traversal (..) not allowed: " + path);
        }
        if (!path.startsWith("/")) {
            throw new IllegalArgumentException("Dump path must be absolute: " + path);
        }
        // Resolve symlinks in parent directory to prevent symlink-based escapes
        java.nio.file.Path filePath = java.nio.file.Path.of(path);
        java.nio.file.Path parentDir = filePath.getParent();
        if (parentDir == null || !java.nio.file.Files.isDirectory(parentDir)) {
            throw new IllegalArgumentException("Parent directory does not exist: " + path);
        }
        String resolvedPath;
        try {
            java.nio.file.Path realParent = parentDir.toRealPath();
            resolvedPath = realParent.resolve(filePath.getFileName()).toString();
        } catch (java.io.IOException e) {
            throw new IllegalArgumentException("Cannot resolve path: " + path, e);
        }
        // Validate resolved path against allowed directories
        boolean allowed = false;
        for (String dir : allowedDumpDirs) {
            if (resolvedPath.startsWith(dir + "/") || resolvedPath.equals(dir)) {
                allowed = true;
                break;
            }
        }
        if (!allowed) {
            throw new IllegalArgumentException(
                "Dump path [" + path + "] (resolved: [" + resolvedPath + "]) not under allowed directories: " + allowedDumpDirs
            );
        }
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment cPath = arena.allocateFrom(resolvedPath);
            long rc = (long) DUMP.invokeExact(cPath);
            NativeLibraryLoader.checkResult(rc);
            logger.info("jemalloc heap profile dumped to {}", resolvedPath);
            return resolvedPath;
        } catch (Throwable t) {
            throw new RuntimeException("Failed to dump heap profile to " + resolvedPath, t);
        }
    }

    @Override
    public void reset(int lgSample) {
        if (lgSample < 0 || lgSample > 30) {
            throw new IllegalArgumentException("lg_prof_sample must be between 0 and 30");
        }
        try {
            long rc = (long) RESET.invokeExact((long) lgSample);
            NativeLibraryLoader.checkResult(rc);
            logger.info("jemalloc profiling reset with lg_prof_sample={}", lgSample);
        } catch (Throwable t) {
            throw new RuntimeException("Failed to reset profiling", t);
        }
    }

    @Override
    public boolean isActive() {
        return active;
    }
}
