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

package org.opensearch.index.snapshots.blobstore;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.opensearch.test.OpenSearchTestCase;
import org.hamcrest.MatcherAssert;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;

public class SlicedInputStreamTests extends OpenSearchTestCase {
    public void testReadRandom() throws IOException {
        int parts = randomIntBetween(1, 20);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        int numWriteOps = scaledRandomIntBetween(1000, 10000);
        final long seed = randomLong();
        Random random = new Random(seed);
        for (int i = 0; i < numWriteOps; i++) {
            switch (random.nextInt(5)) {
                case 1:
                    stream.write(random.nextInt(Byte.MAX_VALUE));
                    break;
                default:
                    stream.write(randomBytes(random));
                    break;
            }
        }

        final CheckClosedInputStream[] streams = new CheckClosedInputStream[parts];
        byte[] bytes = stream.toByteArray();
        int slice = bytes.length / parts;
        int offset = 0;
        int length;
        for (int i = 0; i < parts; i++) {
            length = i == parts - 1 ? bytes.length - offset : slice;
            streams[i] = new CheckClosedInputStream(new ByteArrayInputStream(bytes, offset, length));
            offset += length;
        }

        SlicedInputStream input = new SlicedInputStream(parts) {
            @Override
            protected InputStream openSlice(int slice) throws IOException {
                return streams[slice];
            }
        };
        random = new Random(seed);
        assertThat(input.available(), equalTo(streams[0].available()));
        for (int i = 0; i < numWriteOps; i++) {
            switch (random.nextInt(5)) {
                case 1:
                    assertThat(random.nextInt(Byte.MAX_VALUE), equalTo(input.read()));
                    break;
                default:
                    byte[] expectedBytes = randomBytes(random);
                    byte[] actualBytes = input.readNBytes(expectedBytes.length);
                    assertArrayEquals(expectedBytes, actualBytes);
                    break;
            }
        }

        assertThat(input.available(), equalTo(0));
        for (int i = 0; i < streams.length - 1; i++) {
            assertTrue(streams[i].closed);
        }
        input.close();

        for (int i = 0; i < streams.length; i++) {
            assertTrue(streams[i].closed);
        }

    }

    public void testReadZeroLength() throws IOException {
        try (InputStream input = createSingleByteStream()) {
            final byte[] buffer = new byte[100];
            final int read = input.read(buffer, 0, 0);
            MatcherAssert.assertThat(read, equalTo(0));
        }
    }

    public void testInvalidOffsetAndLength() throws IOException {
        try (InputStream input = createSingleByteStream()) {
            final byte[] buffer = new byte[100];
            expectThrows(NullPointerException.class, () -> input.read(null, 0, 10));
            expectThrows(IndexOutOfBoundsException.class, () -> input.read(buffer, -1, 10));
            expectThrows(IndexOutOfBoundsException.class, () -> input.read(buffer, 0, -1));
            expectThrows(IndexOutOfBoundsException.class, () -> input.read(buffer, 0, buffer.length + 1));
        }
    }

    public void testReadAllBytes() throws IOException {
        final byte[] expectedResults = randomByteArrayOfLength(50_000);
        final int numSlices = 200;
        final int slizeSize = expectedResults.length / numSlices;

        final List<byte[]> arraySlices = new ArrayList<>(numSlices);
        for (int i = 0; i < numSlices; i++) {
            final int offset = slizeSize * i;
            arraySlices.add(Arrays.copyOfRange(expectedResults, offset, offset + slizeSize));
        }
        // Create a SlicedInputStream that will return the expected result in 2 slices
        final byte[] actualResults;
        try (InputStream is = new SlicedInputStream(numSlices) {
            @Override
            protected InputStream openSlice(int slice) {
                return new ByteArrayInputStream(arraySlices.get(slice));
            }
        }) {
            actualResults = is.readAllBytes();
        }
        assertArrayEquals(expectedResults, actualResults);
    }

    private byte[] randomBytes(Random random) {
        int length = RandomNumbers.randomIntBetween(random, 1, 10);
        byte[] data = new byte[length];
        random.nextBytes(data);
        return data;
    }

    private static InputStream createSingleByteStream() {
        return new SlicedInputStream(1) {
            @Override
            protected InputStream openSlice(int slice) {
                return new ByteArrayInputStream(new byte[] { 1 });
            }
        };
    }

    private static final class CheckClosedInputStream extends FilterInputStream {

        public boolean closed = false;

        CheckClosedInputStream(InputStream in) {
            super(in);
        }

        @Override
        public void close() throws IOException {
            closed = true;
            super.close();
        }
    }
}
