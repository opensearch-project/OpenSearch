/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.identity;

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Util class to convert uuid to bytes and extract uuid from bytes
 * Helpful during stream writes and reads
 */
public class UuidUtils {
    /**
     * Convert a byte array to original UUID for stream-input read
     * @param bytes input bytes to convert to UUID
     * @return uuid
     */
    public static UUID asUuid(byte[] bytes) {
        ByteBuffer bb = ByteBuffer.wrap(bytes);
        long firstLong = bb.getLong();
        long secondLong = bb.getLong();
        return new UUID(firstLong, secondLong);
    }

    /**
     * Convert uuid to byte array for stream-output write
     * @param uuid to be converted to byte array
     * @return byte array
     */
    public static byte[] asBytes(UUID uuid) {
        ByteBuffer bb = ByteBuffer.wrap(new byte[16]);
        bb.putLong(uuid.getMostSignificantBits());
        bb.putLong(uuid.getLeastSignificantBits());
        return bb.array();
    }
}
