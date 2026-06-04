/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.common.SuppressForbidden;

import java.io.IOException;
import java.io.InputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Loads the unified native library and provides a shared {@link SymbolLookup}.
 *
 * <p>Uses the initialization-on-demand holder idiom for thread-safe lazy loading
 * without explicit synchronization. The JVM guarantees that the holder class is
 * initialized exactly once, on first access, with full happens-before semantics.
 *
 * <p>Error convention: FFM functions return {@code i64}. If {@code result < 0},
 * negate it to get a pointer to a heap-allocated error string. Call
 * {@link #checkResult(long)} to automatically convert errors to exceptions.
 */
public final class NativeLibraryLoader {

    private static final String LIBRARY_NAME = "opensearch_native";

    private NativeLibraryLoader() {}

    // ---- Initialization-on-demand holder ----

    /**
     * Holder class for lazy, thread-safe initialization.
     * The JVM guarantees this class is initialized exactly once on first access,
     * with full happens-before semantics — no volatile, no synchronized needed.
     */
    private static final class Holder {
        static final SymbolLookup LOOKUP;
        static final MethodHandle ERROR_MESSAGE;
        static final MethodHandle ERROR_FREE;

        static {
            LOOKUP = loadLibrary();
            Linker linker = Linker.nativeLinker();
            ERROR_MESSAGE = linker.downcallHandle(
                LOOKUP.find("native_error_message").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
            );
            ERROR_FREE = linker.downcallHandle(
                LOOKUP.find("native_error_free").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
            );
            // Register the Rust→Java log callback
            LOOKUP.find("native_logger_init").ifPresent(sym -> RustLoggerBridge.register(linker, sym));
        }
    }

    /** Returns the shared {@link SymbolLookup}. Loads the library on first call. */
    public static SymbolLookup symbolLookup() {
        return Holder.LOOKUP;
    }

    /**
     * Returns {@code true} if the native library has been (or can be) successfully loaded.
     * This method does not throw — it returns {@code false} if loading fails.
     */
    public static boolean isLoaded() {
        try {
            symbolLookup();
            return true;
        } catch (Throwable t) {
            return false;
        }
    }

    // ---- Error handling ----

    /**
     * Reads a native error message from the given error pointer and frees it.
     * The error pointer must have been produced by Rust's {@code into_error_ptr}.
     *
     * @param errPtr the positive error pointer (already negated by the caller)
     * @return the error message string
     */
    private static String readAndFreeError(long errPtr) {
        if (errPtr == 0) {
            return "native error with null pointer";
        }
        String msg;
        try {
            MemorySegment msgSeg = (MemorySegment) Holder.ERROR_MESSAGE.invokeExact(errPtr);
            // CString is null-terminated — reinterpret to max addressable range,
            // getString stops at the null terminator. This is the standard FFM
            // pattern for reading C strings of unknown length.
            msg = msgSeg.reinterpret(Long.MAX_VALUE).getString(0);
        } catch (Throwable t) {
            msg = "failed to read native error (ptr=0x" + Long.toHexString(errPtr) + ")";
        } finally {
            try {
                Holder.ERROR_FREE.invokeExact(errPtr);
            } catch (Throwable ignored) {}
        }
        return msg;
    }

    /**
     * Checks an FFM result. If {@code >= 0}, returns it. If {@code < 0}, reads
     * the native error message, frees it, and throws a {@link RuntimeException}.
     */
    public static long checkResult(long result) {
        if (result >= 0) {
            return result;
        }
        throw new RuntimeException(readAndFreeError(-result));
    }

    /**
     * Same as {@link #checkResult} but throws {@link IOException}.
     */
    public static long checkResultIO(long result) throws IOException {
        if (result >= 0) {
            return result;
        }
        throw new IOException(readAndFreeError(-result));
    }

    // ---- Library loading ----

    @SuppressForbidden(reason = "Needs temp directory to extract native library from classpath")
    private static SymbolLookup loadLibrary() {
        String libFile = PlatformHelper.getPlatformLibraryName(LIBRARY_NAME);

        // Try java.library.path
        String javaLibPath = System.getProperty("java.library.path", "");
        for (String dir : javaLibPath.split(System.getProperty("path.separator"))) {
            if (dir.isEmpty()) {
                continue;
            }
            Path candidate = Path.of(dir, libFile);
            if (Files.exists(candidate)) {
                return SymbolLookup.libraryLookup(candidate, Arena.global());
            }
        }

        // Try classpath resources
        String platformDir = PlatformHelper.getPlatformDirectory();
        String resourcePath = "/native/" + platformDir + "/" + libFile;
        try (InputStream is = NativeLibraryLoader.class.getResourceAsStream(resourcePath)) {
            if (is != null) {
                Path tempDir = Files.createTempDirectory("opensearch-native-");
                Path tempLib = tempDir.resolve(libFile);
                Files.copy(is, tempLib, StandardCopyOption.REPLACE_EXISTING);
                tempLib.toFile().setExecutable(true);
                tempLib.toFile().deleteOnExit();
                tempDir.toFile().deleteOnExit();
                return SymbolLookup.libraryLookup(tempLib, Arena.global());
            }
        } catch (IOException e) {
            // fall through
        }

        // Try native.lib.path system property (for tests)
        String nativeLibPath = System.getProperty("native.lib.path");
        if (nativeLibPath != null) {
            return SymbolLookup.libraryLookup(Path.of(nativeLibPath), Arena.global());
        }

        throw new RuntimeException("Failed to load native library '" + LIBRARY_NAME + "'");
    }
}
