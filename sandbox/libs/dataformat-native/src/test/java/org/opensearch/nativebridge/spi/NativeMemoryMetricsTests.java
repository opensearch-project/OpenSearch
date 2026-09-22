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
 * Tests that the native jemalloc metrics functions are available and return valid data.
 */
public class NativeMemoryMetricsTests extends OpenSearchTestCase {

    public void testAllocatedBytesIsPositive() throws Throwable {
        SymbolLookup lookup = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        MethodHandle allocated = linker.downcallHandle(
            lookup.find("native_jemalloc_allocated_bytes").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG)
        );
        long bytes = (long) allocated.invokeExact();
        assertTrue("allocated bytes should be positive, got " + bytes, bytes > 0);
    }

    public void testResidentBytesIsPositive() throws Throwable {
        SymbolLookup lookup = NativeLibraryLoader.symbolLookup();
        Linker linker = Linker.nativeLinker();
        MethodHandle resident = linker.downcallHandle(
            lookup.find("native_jemalloc_resident_bytes").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG)
        );
        long bytes = (long) resident.invokeExact();
        assertTrue("resident bytes should be positive, got " + bytes, bytes > 0);
    }
}
