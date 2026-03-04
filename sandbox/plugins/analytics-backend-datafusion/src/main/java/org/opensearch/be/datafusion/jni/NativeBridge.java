/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.jni;

/**
 * Core JNI bridge to native DataFusion library.
 * All native method declarations are centralized here.
 */
public final class NativeBridge {

    static {
        // TODO : NativeLibraryLoader.load("opensearch_datafusion_jni");
    }

    private NativeBridge() {}

    // Reader management
    public static native long createDatafusionReader(String path, String[] files);

    public static native void closeDatafusionReader(long ptr);
}
