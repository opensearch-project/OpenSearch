/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.nativebridge.spi.NativeLibraryLoader;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;

/**
 * Test helper for creating real native TieredObjectStore pointers via FFM.
 * Avoids a compile dependency on parquet-data-format by calling the native
 * symbols directly through the shared native library.
 */
final class NativeStoreTestHelper {

    private static final MethodHandle TS_CREATE;
    private static final MethodHandle TS_DESTROY;
    private static final MethodHandle TS_GET_BOX_PTR;
    private static final MethodHandle TS_DESTROY_BOX_PTR;

    static {
        var lib = NativeLibraryLoader.symbolLookup();
        var linker = Linker.nativeLinker();
        TS_CREATE = linker.downcallHandle(
            lib.find("ts_create_tiered_object_store").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        TS_DESTROY = linker.downcallHandle(
            lib.find("ts_destroy_tiered_object_store").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        TS_GET_BOX_PTR = linker.downcallHandle(
            lib.find("ts_get_object_store_box_ptr").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
        TS_DESTROY_BOX_PTR = linker.downcallHandle(
            lib.find("ts_destroy_object_store_box_ptr").orElseThrow(),
            FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG)
        );
    }

    private NativeStoreTestHelper() {}

    /** Create a TieredObjectStore. Pass 0 for default LocalFileSystem / no remote. */
    static long createTieredObjectStore(long localPtr, long remotePtr) {
        try {
            return NativeLibraryLoader.checkResult((long) TS_CREATE.invokeExact(localPtr, remotePtr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to create TieredObjectStore", t);
        }
    }

    /** Destroy a TieredObjectStore. */
    static void destroyTieredObjectStore(long ptr) {
        try {
            NativeLibraryLoader.checkResult((long) TS_DESTROY.invokeExact(ptr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to destroy TieredObjectStore", t);
        }
    }

    /** Get a Box pointer (ObjectStore trait object) for DataFusion from a TieredObjectStore. */
    static long getObjectStoreBoxPtr(long tieredStorePtr) {
        try {
            return NativeLibraryLoader.checkResult((long) TS_GET_BOX_PTR.invokeExact(tieredStorePtr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to get object store box ptr", t);
        }
    }

    /** Destroy a Box pointer (ObjectStore trait object). */
    static void destroyObjectStoreBoxPtr(long ptr) {
        try {
            NativeLibraryLoader.checkResult((long) TS_DESTROY_BOX_PTR.invokeExact(ptr));
        } catch (Throwable t) {
            throw new RuntimeException("Failed to destroy object store box ptr", t);
        }
    }
}
