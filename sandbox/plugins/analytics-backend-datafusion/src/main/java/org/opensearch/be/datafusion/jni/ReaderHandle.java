/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.jni;

import org.opensearch.analytics.backend.jni.NativeHandle;

/**
 * Type-safe handle for native reader.
 */
public final class ReaderHandle extends NativeHandle {

    private final boolean ownsPointer;

    /**
     * Creates a reader handle by allocating a native DataFusion reader for the given path and files.
     * @param path the directory path containing data files
     * @param files the array of file names to read
     */
    public ReaderHandle(String path, String[] files) {
        super(NativeBridge.createDatafusionReader(path, files));
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
     */
    public static ReaderHandle wrap(long existingPtr) {
        return new ReaderHandle(existingPtr);
    }
}
