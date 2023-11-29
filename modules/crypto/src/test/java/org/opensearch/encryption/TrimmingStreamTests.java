/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class TrimmingStreamTests extends OpenSearchTestCase {

    static class ReadCountInputStreamTest extends ByteArrayInputStream {

        public ReadCountInputStreamTest(byte[] buf) {
            super(buf);
        }

        public int getPos() {
            return pos;
        }
    }

    public void testReadInRange() throws IOException {
        byte[] data = generateRandomData(100);
        ReadCountInputStreamTest input = new ReadCountInputStreamTest(data);

        long sourceStart = generateRandomValue(0, 80);
        long sourceEnd = generateRandomValue(sourceStart, 99);
        long targetStart = generateRandomValue(sourceStart, sourceEnd);
        long targetEnd = generateRandomValue(targetStart, sourceEnd);

        TrimmingStream trimmingStream = new TrimmingStream(sourceStart, sourceEnd, targetStart, targetEnd, input);

        byte[] result = new byte[(int) (sourceEnd - sourceStart + 1)];
        int bytesRead = trimmingStream.read(result, 0, result.length);

        long expectedBytesRead = targetEnd - targetStart + 1;
        assertEquals(expectedBytesRead, bytesRead);
        assertEquals(sourceEnd - sourceStart + 1, input.getPos());
    }

    public void testReadOutsideRange() throws IOException {
        byte[] data = generateRandomData(100);
        ReadCountInputStreamTest input = new ReadCountInputStreamTest(data);

        long sourceStart = generateRandomValue(0, 80);
        long sourceEnd = generateRandomValue(sourceStart, 99);
        long targetStart = generateRandomValue(sourceStart, sourceEnd);
        long targetEnd = generateRandomValue(targetStart, sourceEnd);

        TrimmingStream trimmingStream = new TrimmingStream(sourceStart, sourceEnd, targetStart, targetEnd, input);

        byte[] result = new byte[(int) (targetEnd - targetStart + 1)];
        int bytesRead = trimmingStream.read(result, 0, result.length);

        long expectedBytesRead = targetEnd - targetStart + 1;
        assertEquals(expectedBytesRead, bytesRead);
        assertEquals(sourceEnd - sourceStart + 1, input.getPos());

        // Try to read more bytes, should return -1 (end of stream)
        int additionalBytesRead = trimmingStream.read(result, 0, 50);
        assertEquals(-1, additionalBytesRead);
        assertEquals(sourceEnd - sourceStart + 1, input.getPos());
    }

    public void testSingleByteReadInRange() throws IOException {
        byte[] data = generateRandomData(100);
        ReadCountInputStreamTest input = new ReadCountInputStreamTest(data);

        long sourceStart = generateRandomValue(0, 80);
        long sourceEnd = generateRandomValue(sourceStart, 99);
        long targetStart = generateRandomValue(sourceStart, sourceEnd);
        long targetEnd = generateRandomValue(targetStart, sourceEnd);

        TrimmingStream trimmingStream = new TrimmingStream(sourceStart, sourceEnd, targetStart, targetEnd, input);

        int bytesRead = 0;
        int value;
        while ((value = trimmingStream.read()) != -1) {
            bytesRead++;
        }

        long expectedBytesRead = targetEnd - targetStart + 1;
        assertEquals(expectedBytesRead, bytesRead);
        assertEquals(sourceEnd - sourceStart + 1, input.getPos());
    }

    public void testInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> new TrimmingStream(-10, 60, 20, 40, new ByteArrayInputStream(new byte[100])));
        assertThrows(IllegalArgumentException.class, () -> new TrimmingStream(10, 60, 40, 20, new ByteArrayInputStream(new byte[100])));
    }

    public void testSourceSameAsTarget() throws IOException {
        byte[] data = generateRandomData(100);
        ReadCountInputStreamTest input = new ReadCountInputStreamTest(data);

        long sourceStart = generateRandomValue(0, 80);
        long sourceEnd = generateRandomValue(sourceStart, 99);
        TrimmingStream trimmingStream = new TrimmingStream(sourceStart, sourceEnd, sourceStart, sourceEnd, input);

        byte[] result = new byte[(int) (sourceEnd - sourceStart + 1)];
        int bytesRead = trimmingStream.read(result, 0, result.length);

        assertEquals(sourceEnd - sourceStart + 1, bytesRead);
        assertEquals(sourceEnd - sourceStart + 1, input.getPos());
    }

    private byte[] generateRandomData(int length) {
        byte[] data = new byte[length];
        for (int i = 0; i < length; i++) {
            data[i] = (byte) (Math.random() * 256 - 128);
        }
        return data;
    }

    private long generateRandomValue(long min, long max) {
        return min + (long) (Math.random() * (max - min + 1));
    }
}
