/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.hash;

import org.apache.lucene.util.StringHelper;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;

public class HashFunctionTestCaseTests extends OpenSearchTestCase {
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);

    /**
     * Asserts the positive case where a hash function passes the avalanche test.
     */
    public void testStrongHashFunction() {
        HashFunctionTestCase murmur3 = new HashFunctionTestCase() {
            private final byte[] scratch = new byte[4];

            @Override
            public byte[] hash(byte[] input) {
                int hash = StringHelper.murmurhash3_x86_32(input, 0, input.length, StringHelper.GOOD_FAST_HASH_SEED);
                INT_HANDLE.set(scratch, 0, hash);
                return scratch;
            }

            @Override
            public int outputBits() {
                return 32;
            }
        };

        murmur3.testAvalanche();
    }

    /**
     * Asserts the negative case where a hash function fails the avalanche test.
     */
    public void testWeakHashFunction() {
        HashFunctionTestCase arraysHashCode = new HashFunctionTestCase() {
            private final byte[] scratch = new byte[4];

            @Override
            public byte[] hash(byte[] input) {
                int hash = Arrays.hashCode(input);
                INT_HANDLE.set(scratch, 0, hash);
                return scratch;
            }

            @Override
            public int outputBits() {
                return 32;
            }
        };

        AssertionError ex = expectThrows(AssertionError.class, arraysHashCode::testAvalanche);
        assertTrue(ex.getMessage().contains("bias exceeds threshold"));
    }
}
