/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;

/**
 * Bridge that allows Rust to log through Java's Log4j via an FFM callback.
 *
 * <p>At startup, {@link NativeLibraryLoader} registers {@link #log} as a function pointer
 * with the native library. Rust calls it directly — no JNI.
 *
 * <p>Level short-circuit: the Rust {@code log_debug!}/{@code log_info!} macros gate on a
 * Rust-side atomic level BEFORE {@code format!}, so a suppressed log costs only an atomic
 * load + branch (no string allocation, no FFM crossing). Java keeps that atomic in sync
 * via {@link #pushLevel()} — called at registration and re-pushed on every upcall if the
 * level has drifted (detecting dynamic changes from {@code _cluster/settings}).
 */
public class RustLoggerBridge {

    private static final Logger logger = LogManager.getLogger(RustLoggerBridge.class);

    /** Level codes shared with Rust's {@code LogLevel}: 0=Debug, 1=Info, 2=Error. */
    private static final int LEVEL_DEBUG = 0;
    private static final int LEVEL_INFO = 1;
    private static final int LEVEL_ERROR = 2;

    /** Bound downcall to Rust's {@code native_logger_set_level(int)}; null until registered. */
    private static volatile MethodHandle setLevelHandle;

    /**
     * Called from Rust via the registered function pointer.
     * Signature must match: {@code void(int level, MemorySegment msgPtr, long msgLen)}
     */
    static void log(int level, MemorySegment msgPtr, long msgLen) {
        String message = new String(msgPtr.reinterpret(msgLen).toArray(ValueLayout.JAVA_BYTE), StandardCharsets.UTF_8);
        switch (level) {
            case 0 -> logger.debug(message);
            case 1 -> logger.info(message);
            case 2 -> logger.error(message);
            default -> logger.warn(message);
        }
    }

    /**
     * Creates an upcall stub for {@link #log} and registers it with the native library.
     * Called once by {@link NativeLibraryLoader}.
     */
    static void register(Linker linker, MemorySegment initLoggerSymbol) {
        try {
            MethodHandle logHandle = MethodHandles.lookup()
                .findStatic(RustLoggerBridge.class, "log", MethodType.methodType(void.class, int.class, MemorySegment.class, long.class));
            MemorySegment upcallStub = linker.upcallStub(
                logHandle,
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG),
                Arena.global()
            );
            MethodHandle initLogger = linker.downcallHandle(initLoggerSymbol, FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
            initLogger.invokeExact(upcallStub);
        } catch (Throwable t) {
            logger.error("Failed to register native logger callback", t);
        }
    }

    /**
     * Binds Rust's {@code native_logger_set_level(int)} and pushes the current level.
     * Called once by {@link NativeLibraryLoader} after {@link #register}.
     * Dynamic level changes are handled by a cluster settings listener that calls
     * {@link #pushLevel()} when any {@code logger.*} setting is updated.
     */
    static void registerSetLevel(Linker linker, MemorySegment setLevelSymbol) {
        try {
            setLevelHandle = linker.downcallHandle(setLevelSymbol, FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT));
            pushLevel();
        } catch (Throwable t) {
            logger.error("Failed to register native logger set-level callback", t);
        }
    }

    /**
     * Push this class's current Log4j level to Rust so the macros short-circuit
     * suppressed levels. Called at registration and by the cluster settings listener.
     */
    public static void pushLevel() {
        MethodHandle handle = setLevelHandle;
        if (handle == null) {
            return;
        }
        int level = toRustLevel(logger.getLevel());
        try {
            handle.invokeExact(level);
        } catch (Throwable t) {
            logger.error("Failed to push native log level", t);
        }
    }

    /** Map a Log4j {@link Level} to the Rust LogLevel int (0=Debug, 1=Info, 2=Error). */
    private static int toRustLevel(Level level) {
        if (level.intLevel() >= Level.DEBUG.intLevel()) {
            return LEVEL_DEBUG;
        } else if (level.intLevel() >= Level.INFO.intLevel()) {
            return LEVEL_INFO;
        }
        return LEVEL_ERROR;
    }

    private RustLoggerBridge() {}
}
