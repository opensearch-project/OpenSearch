/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common;

import org.apache.lucene.util.BytesRef;

/**
 * A set of utilities for {@link BytesRef}
 *
 * @opensearch.internal
 */
public final class BytesRefUtils {
    private BytesRefUtils() {}

    public static long bytesToLong(BytesRef bytes) {
        int high = (bytes.bytes[bytes.offset + 0] << 24) | ((bytes.bytes[bytes.offset + 1] & 0xff) << 16) | ((bytes.bytes[bytes.offset + 2]
            & 0xff) << 8) | (bytes.bytes[bytes.offset + 3] & 0xff);
        int low = (bytes.bytes[bytes.offset + 4] << 24) | ((bytes.bytes[bytes.offset + 5] & 0xff) << 16) | ((bytes.bytes[bytes.offset + 6]
            & 0xff) << 8) | (bytes.bytes[bytes.offset + 7] & 0xff);
        return (((long) high) << 32) | (low & 0x0ffffffffL);
    }
}
