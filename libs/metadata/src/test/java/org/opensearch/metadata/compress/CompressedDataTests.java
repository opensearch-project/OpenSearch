/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.compress;

import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class CompressedDataTests extends OpenSearchTestCase {

    private void assertEquals(CompressedData d1, CompressedData d2) {
        assertThat(d1, equalTo(d2));
        assertArrayEquals(d1.compressedBytes(), d2.compressedBytes());
        assertThat(d1.checksum(), equalTo(d2.checksum()));
        assertThat(d1.hashCode(), equalTo(d2.hashCode()));
    }

    public void testSerialization() throws IOException {
        final CompressedData before = createTestItem();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final CompressedData after = new CompressedData(in);

        assertEquals(before, after);
    }

    public void testSimple() {
        byte[] bytes = "this is a simple byte array".getBytes();
        int checksum = 12345;

        CompressedData data = new CompressedData(bytes, checksum);
        assertArrayEquals(data.compressedBytes(), bytes);
        assertThat(data.checksum(), equalTo(checksum));

        CompressedData data2 = new CompressedData(bytes.clone(), checksum);
        assertEquals(data, data2);

        byte[] bytes2 = "this is a different byte array".getBytes();
        CompressedData data3 = new CompressedData(bytes2, checksum);
        assertThat(data3.compressedBytes(), not(equalTo(bytes)));
        assertThat(data3, not(equalTo(data)));
    }

    public void testRandom() throws IOException {
        Random r = random();
        for (int i = 0; i < 100; i++) {
            byte[] bytes = randomByteArrayOfLength(r.nextInt(10000) + 1);
            int checksum = r.nextInt();

            CompressedData data = new CompressedData(bytes, checksum);
            assertArrayEquals(data.compressedBytes(), bytes);
            assertThat(data.checksum(), equalTo(checksum));

            // Test serialization round-trip
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final StreamOutput out = new OutputStreamStreamOutput(baos);
            data.writeTo(out);
            out.close();

            final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            final StreamInput in = new InputStreamStreamInput(bais);
            final CompressedData deserialized = new CompressedData(in);

            assertEquals(data, deserialized);
        }
    }

    public void testHashCode() {
        CompressedData data1 = new CompressedData(new byte[] { 1, 2, 3 }, 100);
        CompressedData data2 = new CompressedData(new byte[] { 1, 2, 3 }, 200);

        // Different checksums should produce different hashCodes
        assertNotEquals(data1.hashCode(), data2.hashCode());
    }

    public void testEqualsWithSameBytes() {
        byte[] bytes = randomByteArrayOfLength(50);
        int checksum = randomInt();

        CompressedData data1 = new CompressedData(bytes, checksum);
        CompressedData data2 = new CompressedData(bytes.clone(), checksum);

        assertEquals(data1, data2);
    }

    public void testNotEqualsWithDifferentBytes() {
        int checksum = randomInt();

        CompressedData data1 = new CompressedData(new byte[] { 1, 2, 3 }, checksum);
        CompressedData data2 = new CompressedData(new byte[] { 4, 5, 6 }, checksum);

        assertThat(data1, not(equalTo(data2)));
    }

    public void testNotEqualsWithDifferentChecksum() {
        byte[] bytes = new byte[] { 1, 2, 3 };

        CompressedData data1 = new CompressedData(bytes, 100);
        CompressedData data2 = new CompressedData(bytes.clone(), 200);

        assertThat(data1, not(equalTo(data2)));
    }

    private static CompressedData createTestItem() {
        return new CompressedData(randomByteArrayOfLength(randomIntBetween(10, 100)), randomInt());
    }
}
