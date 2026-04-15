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
 */
public class RustLoggerBridge {

    private static final Logger logger = LogManager.getLogger(RustLoggerBridge.class);

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

    private RustLoggerBridge() {}
}
