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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * Loads the unified native library and provides a shared {@link SymbolLookup}.
 *
 * <p>Error convention: FFM functions return {@code i64}. If {@code result < 0},
 * negate it to get a pointer to a heap-allocated error string. Call
 * {@link #checkResult(long)} to automatically convert errors to exceptions.
 */
public final class NativeLibraryLoader {

    private static final String LIBRARY_NAME = "opensearch_native";
    private static volatile SymbolLookup lookup;
    private static MethodHandle ERROR_MESSAGE;
    private static MethodHandle ERROR_FREE;

    private NativeLibraryLoader() {}

    /** Returns the shared {@link SymbolLookup}. Loads the library on first call. */
    public static synchronized SymbolLookup symbolLookup() {
        if (lookup == null) {
            lookup = loadLibrary();
            Linker linker = Linker.nativeLinker();
            ERROR_MESSAGE = linker.downcallHandle(
                lookup.find("native_error_message").orElseThrow(),
                FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
            );
            ERROR_FREE = linker.downcallHandle(
                lookup.find("native_error_free").orElseThrow(),
                FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG)
            );
        }
        return lookup;
    }

    /**
     * Checks an FFM result. If {@code >= 0}, returns it. If {@code < 0}, reads
     * the native error message, frees it, and throws a {@link RuntimeException}.
     */
    public static long checkResult(long result) {
        if (result >= 0) {
            return result;
        }
        long errPtr = -result;
        String msg;
        try {
            MemorySegment msgSeg = (MemorySegment) ERROR_MESSAGE.invokeExact(errPtr);
            msg = msgSeg.reinterpret(4096).getString(0);
        } catch (Throwable t) {
            msg = "failed to read native error";
        }
        try {
            ERROR_FREE.invokeExact(errPtr);
        } catch (Throwable ignored) {}
        throw new RuntimeException(msg);
    }

    /**
     * Same as {@link #checkResult} but throws {@link IOException}.
     */
    public static long checkResultIO(long result) throws IOException {
        if (result >= 0) {
            return result;
        }
        long errPtr = -result;
        String msg;
        try {
            MemorySegment msgSeg = (MemorySegment) ERROR_MESSAGE.invokeExact(errPtr);
            msg = msgSeg.reinterpret(4096).getString(0);
        } catch (Throwable t) {
            msg = "failed to read native error";
        }
        try {
            ERROR_FREE.invokeExact(errPtr);
        } catch (Throwable ignored) {}
        throw new IOException(msg);
    }

    @SuppressForbidden(reason = "Needs temp directory to extract native library from classpath")
    private static SymbolLookup loadLibrary() {
        String libFile = PlatformHelper.getPlatformLibraryName(LIBRARY_NAME);

        // Try java.library.path
        String javaLibPath = System.getProperty("java.library.path", "");
        for (String dir : javaLibPath.split(System.getProperty("path.separator"))) {
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
