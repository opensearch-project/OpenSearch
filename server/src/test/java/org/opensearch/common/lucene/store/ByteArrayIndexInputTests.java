/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common.lucene.store;

import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.Matchers.containsString;

public class ByteArrayIndexInputTests extends OpenSearchIndexInputTestCase {
    public void testRandomReads() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            ByteArrayIndexInput indexInput = new ByteArrayIndexInput("test", input);
            assertEquals(input.length, indexInput.length());
            assertEquals(0, indexInput.getFilePointer());
            byte[] output = randomReadAndSlice(indexInput, input.length);
            assertArrayEquals(input, output);
        }
    }

    public void testRandomOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            ByteArrayIndexInput indexInput = new ByteArrayIndexInput("test", input);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            int bytesLeft = input.length - firstReadLen;
            try {
                // read using int size
                int secondReadLen = bytesLeft + randomIntBetween(1, 100);
                indexInput.readBytes(new byte[secondReadLen], 0, secondReadLen);
                fail();
            } catch (IOException ex) {
                assertThat(ex.getMessage(), containsString("EOF"));
            }
        }
    }

    public void testSeekOverflow() throws IOException {
        for (int i = 0; i < 100; i++) {
            byte[] input = randomUnicodeOfLength(randomIntBetween(1, 1000)).getBytes(StandardCharsets.UTF_8);
            ByteArrayIndexInput indexInput = new ByteArrayIndexInput("test", input);
            int firstReadLen = randomIntBetween(0, input.length - 1);
            randomReadAndSlice(indexInput, firstReadLen);
            try {
                switch (randomIntBetween(0, 2)) {
                    case 0:
                        indexInput.seek(Integer.MAX_VALUE + 4L);
                        break;
                    case 1:
                        indexInput.seek(-randomIntBetween(1, 10));
                        break;
                    case 2:
                        int seek = input.length + randomIntBetween(1, 100);
                        indexInput.seek(seek);
                        break;
                    default:
                        fail();
                }
                fail();
            } catch (IOException ex) {
                assertThat(ex.getMessage(), containsString("EOF"));
            } catch (IllegalArgumentException ex) {
                assertThat(ex.getMessage(), containsString("negative position"));
            }
        }
    }

    public void testRandomReadEdges() throws IOException {
        final int size = 16;
        byte[] input = Arrays.copyOfRange(randomUnicodeOfLength(size).getBytes(StandardCharsets.UTF_8), 0, size);
        ByteArrayIndexInput indexInput = new ByteArrayIndexInput("test", input);
        assertThrows(EOFException.class, () -> indexInput.readByte(-1));
        assertThrows(EOFException.class, () -> indexInput.readShort(-1));
        assertThrows(EOFException.class, () -> indexInput.readInt(-1));
        assertThrows(EOFException.class, () -> indexInput.readLong(-1));
        for (int i = 0; i < 10; i++) {
            indexInput.readByte(size - Byte.BYTES);
            indexInput.readShort(size - Short.BYTES);
            indexInput.readInt(size - Integer.BYTES);
            indexInput.readLong(size - Long.BYTES);
            indexInput.readByte(0);
            indexInput.readShort(0);
            indexInput.readInt(0);
            indexInput.readLong(0);
        }
        assertThrows(EOFException.class, () -> indexInput.readByte(size));
        assertThrows(EOFException.class, () -> indexInput.readShort(size - Short.BYTES + 1));
        assertThrows(EOFException.class, () -> indexInput.readInt(size - Integer.BYTES + 1));
        assertThrows(EOFException.class, () -> indexInput.readLong(size - Long.BYTES + 1));
    }

    public void testRandomAccessReads() throws IOException {
        byte[] bytes = {
            (byte) 85,  // 01010101
            (byte) 46,  // 00101110
            (byte) 177, // 10110001
            (byte) 36,  // 00100100
            (byte) 231, // 11100111
            (byte) 48,  // 00110000
            (byte) 137, // 10001001
            (byte) 37,  // 00100101
            (byte) 137  // 10001001
        };
        ByteArrayIndexInput indexInput = new ByteArrayIndexInput("test", bytes);
        // 00101110 01010101
        assertEquals(11861, indexInput.readShort(0));
        // 10110001 00101110
        assertEquals(-20178, indexInput.readShort(1));
        // 00100101 10001001
        assertEquals(9609, indexInput.readShort(6));
        // 00100100 10110001 00101110 01010101
        assertEquals(615591509, indexInput.readInt(0));
        // 00110000 11100111 00100100 10110001
        assertEquals(820454577, indexInput.readInt(2));
        // 00100101 10001001 00110000 11100111
        assertEquals(629747943, indexInput.readInt(4));
        // 00100101 10001001 00110000 11100111 00100100 10110001 00101110 01010101
        assertEquals(2704746820523863637L, indexInput.readLong(0));
        // 10001001 00100101 10001001 00110000 11100111 00100100 10110001 00101110
        assertEquals(-8564288273245753042L, indexInput.readLong(1));
    }

    public void testReadBytesWithSlice() throws IOException {
        int inputLength = randomIntBetween(100, 1000);

        byte[] input = randomUnicodeOfLength(inputLength).getBytes(StandardCharsets.UTF_8);
        ByteArrayIndexInput indexInput = new ByteArrayIndexInput("test", input);

        int sliceOffset = randomIntBetween(1, inputLength - 10);
        int sliceLength = randomIntBetween(2, inputLength - sliceOffset);
        IndexInput slice = indexInput.slice("slice", sliceOffset, sliceLength);

        // read a byte from sliced index input and verify if the read value is correct
        assertEquals(input[sliceOffset], slice.readByte());

        // read few more bytes into a byte array
        int bytesToRead = randomIntBetween(1, sliceLength - 1);
        slice.readBytes(new byte[bytesToRead], 0, bytesToRead);

        // now try to read beyond the boundary of the slice, but within the
        // boundary of the original IndexInput. We've already read few bytes
        // so this is expected to fail
        assertThrows(EOFException.class, () -> slice.readBytes(new byte[sliceLength], 0, sliceLength));

        // seek to EOF and then try to read
        slice.seek(sliceLength);
        assertThrows(EOFException.class, () -> slice.readBytes(new byte[1], 0, 1));

        slice.close();
        indexInput.close();
    }
}
