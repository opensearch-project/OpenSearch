/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except
 * in compliance with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.opensearch.encryption.frame.core;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

/** Internal utility methods. */
public final class Utils {
    // SecureRandom objects can both be expensive to initialize and incur synchronization costs.
    // This allows us to minimize both initializations and keep SecureRandom usage thread local
    // to avoid lock contention.
    private static final ThreadLocal<SecureRandom> LOCAL_RANDOM = new ThreadLocal<SecureRandom>() {
        @Override
        protected SecureRandom initialValue() {
            final SecureRandom rnd = new SecureRandom();
            rnd.nextBoolean(); // Force seeding
            return rnd;
        }
    };

    private Utils() {
        // Prevent instantiation
    }

    /**
     * Throws {@link NullPointerException} with message {@code paramName} if {@code object} is null.
     *
     * @param object value to be null-checked
     * @param paramName message for the potential {@link NullPointerException}
     * @return {@code object}
     * @throws NullPointerException if {@code object} is null
     * @param <T> Type of object on which null check is to be performed
     */
    public static <T> T assertNonNull(final T object, final String paramName) throws NullPointerException {
        if (object == null) {
            throw new NullPointerException(paramName + " must not be null");
        }
        return object;
    }

    public static SecureRandom getSecureRandom() {
        return LOCAL_RANDOM.get();
    }

    /**
     * Generate the AAD bytes to use when encrypting/decrypting content. The generated AAD is a block
     * of bytes containing the provided message identifier, the string identifier, the sequence
     * number, and the length of the content.
     *
     * @param messageId the unique message identifier for the ciphertext.
     * @param idString the string describing the type of content processed.
     * @param seqNum the sequence number.
     * @param len the length of the content.
     * @return the bytes containing the generated AAD.
     */
    static byte[] generateContentAad(final byte[] messageId, final String idString, final int seqNum, final long len) {
        final byte[] idBytes = idString.getBytes(StandardCharsets.UTF_8);
        final int aadLen = messageId.length + idBytes.length + Integer.SIZE / Byte.SIZE + Long.SIZE / Byte.SIZE;
        final ByteBuffer aad = ByteBuffer.allocate(aadLen);

        aad.put(messageId);
        aad.put(idBytes);
        aad.putInt(seqNum);
        aad.putLong(len);

        return aad.array();
    }

    public static IllegalArgumentException cannotBeNegative(String field) {
        return new IllegalArgumentException(field + " cannot be negative");
    }

    /**
     * Equivalent to calling {@link ByteBuffer#position(int)} but in a manner which is safe when
     * compiled on Java 9 or newer but used on Java 8 or older.
     * @param buff on which position needs to be set.
     * @param newPosition New position to be set
     * @return {@link ByteBuffer} object with new position set.
     */
    public static ByteBuffer position(final ByteBuffer buff, final int newPosition) {
        ((Buffer) buff).position(newPosition);
        return buff;
    }
}
