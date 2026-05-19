/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.nativelib;

import org.opensearch.analytics.backend.jni.NativeHandle;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.plugins.NativeStoreHandle;

import java.util.List;

/**
 * Type-safe handle for native reader.
 */
public final class ReaderHandle extends NativeHandle {

    private final boolean ownsPointer;

    /**
     * Creates a reader handle by allocating a native DataFusion reader for the given path
     * and {@link MonoFileWriterSet}s. Each entry represents one parquet segment — a single
     * file paired with its writer generation (sourced from the catalog snapshot).
     *
     * @param path the directory path containing data files
     * @param segments the per-segment file sets to read
     * @param dataformatAwareStoreHandle per-format native store handle (null = local, live = use store pointer)
     */
    public ReaderHandle(String path, List<MonoFileWriterSet> segments, NativeStoreHandle dataformatAwareStoreHandle) {
        super(NativeBridge.createDatafusionReader(path, segments, dataformatAwareStoreHandle));
        this.ownsPointer = true;
    }

    /** Wraps an existing pointer without taking ownership. */
    private ReaderHandle(long existingPtr) {
        super(existingPtr);
        this.ownsPointer = false;
    }

    @Override
    protected void doClose() {
        if (ownsPointer) {
            NativeBridge.closeDatafusionReader(ptr);
        }
    }

    /**
     * Wraps a pre-existing native pointer without taking ownership (test only).
     * @param existingPtr the native pointer to wrap
     */
    public static ReaderHandle wrap(long existingPtr) {
        return new ReaderHandle(existingPtr);
    }
}
