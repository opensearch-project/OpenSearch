/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.utils;

import software.amazon.awssdk.crt.CRT;
import software.amazon.awssdk.crt.CRT.UnknownPlatformException;

/**
 * An utility class to check if AWS CRT client is available on the current platform and architecture.
 */
public final class AwsCrtUtils {
    static final boolean isAwsCrtAvailable;

    static {
        boolean crtAvailable;
        try {
            final String archIdentifier = CRT.getArchIdentifier();
            crtAvailable = (archIdentifier != null && archIdentifier.isBlank() == false);
        } catch (ExceptionInInitializerError | UnknownPlatformException ex) {
            crtAvailable = false;
        }
        // CRT native libraries are not available on some platforms
        isAwsCrtAvailable = crtAvailable;
    }

    private AwsCrtUtils() {

    }

    /**
     * Check if AWS CRT client is available on the current platform and architecture.
     *
     * @return true if AWS CRT client is available
     */
    public static boolean isAwsCrtAvailable() {
        return isAwsCrtAvailable;
    }
}
