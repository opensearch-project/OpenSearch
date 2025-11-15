/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * JNI bridge layer for DataFusion native library integration.
 *
 * <p>This package provides:
 * <ul>
 *   <li>Type-safe native handle wrappers ({@link org.opensearch.datafusion.jni.handle.NativeHandle})</li>
 *   <li>Centralized native method declarations ({@link org.opensearch.datafusion.jni.NativeBridge})</li>
 *   <li>Native library loading ({@link org.opensearch.datafusion.jni.NativeLibraryLoader})</li>
 *   <li>JNI exception handling ({@link org.opensearch.datafusion.jni.NativeException})</li>
 * </ul>
 *
 */
package org.opensearch.datafusion.jni;

