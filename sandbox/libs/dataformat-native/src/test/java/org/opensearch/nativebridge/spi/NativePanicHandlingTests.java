/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.nativebridge.spi;

import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Tests that native panics and errors are properly caught and surfaced as Java exceptions.
 */
public class NativePanicHandlingTests extends OpenSearchTestCase {

    private static final MethodHandle TEST_PANIC;
    private static final MethodHandle TEST_ERROR;

    static {
        SymbolLookup lib = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        TEST_PANIC = linker.downcallHandle(
            lib.find("native_test_panic").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
        TEST_ERROR = linker.downcallHandle(
            lib.find("native_test_error").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
        );
    }

    public void testPanicIsCaughtAsException() {
        RuntimeException ex = expectThrows(RuntimeException.class, () -> {
            try (var call = new NativeCall()) {
                call.invoke(TEST_PANIC, call.str("test panic message"), NativeCall.len("test panic message"));
            }
        });
        assertTrue("Should contain panic message, got: " + ex.getMessage(), ex.getMessage().contains("test panic message"));
    }

    public void testErrorIsCaughtAsException() {
        RuntimeException ex = expectThrows(RuntimeException.class, () -> {
            try (var call = new NativeCall()) {
                call.invoke(TEST_ERROR, call.str("test error message"), NativeCall.len("test error message"));
            }
        });
        assertTrue("Should contain error message, got: " + ex.getMessage(), ex.getMessage().contains("test error message"));
    }

    public void testPanicMessagePreservedThroughCheckResultIO() {
        Exception ex = expectThrows(Exception.class, () -> {
            try (var call = new NativeCall()) {
                call.invokeIO(TEST_PANIC, call.str("io panic test"), NativeCall.len("io panic test"));
            }
        });
        assertTrue("Should contain panic message, got: " + ex.getMessage(), ex.getMessage().contains("io panic test"));
    }

    public void testSuccessResultPassesThrough() throws Throwable {
        // native_test_error with empty string still returns negative, so test with a known-good function.
        // Use native_error_free(0) which is a no-op that doesn't crash — but it returns void.
        // Instead, just verify that checkResult(0) returns 0 and checkResult(42) returns 42.
        assertEquals(0L, NativeLibraryLoader.checkResult(0));
        assertEquals(42L, NativeLibraryLoader.checkResult(42));
        assertEquals(Long.MAX_VALUE, NativeLibraryLoader.checkResult(Long.MAX_VALUE));
    }
}
