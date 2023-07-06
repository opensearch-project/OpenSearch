/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.encryption.frame.core;

public final class Constants {
    /**
     * Default length of the message identifier used to uniquely identify every ciphertext created by
     * this library.
     *
     */
    @Deprecated
    public static final int MESSAGE_ID_LEN = 16;

    private Constants() {
        // Prevent instantiation
    }

    /** Marker for identifying the final frame. */
    public static final int ENDFRAME_SEQUENCE_NUMBER = ~0; // is 0xFFFFFFFF

    /**
     * The identifier for non-final frames in the framing content type. This value is used as part of
     * the additional authenticated data (AAD) when encryption of content in a frame.
     */
    public static final String FRAME_STRING_ID = "AWSKMSEncryptionClient Frame";

    /**
     * The identifier for the final frame in the framing content type. This value is used as part of
     * the additional authenticated data (AAD) when encryption of content in a frame.
     */
    public static final String FINAL_FRAME_STRING_ID = "AWSKMSEncryptionClient Final Frame";

    public static final long MAX_FRAME_NUMBER = (1L << 32) - 1;
}
