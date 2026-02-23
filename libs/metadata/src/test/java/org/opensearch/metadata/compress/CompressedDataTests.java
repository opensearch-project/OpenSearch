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
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class CompressedDataTests extends OpenSearchTestCase {

    private void assertEquals(CompressedData d1, CompressedData d2) {
        assertThat(d1, equalTo(d2));
        assertArrayEquals(d1.compressedBytes(), d2.compressedBytes());
        assertThat(d1.checksum(), equalTo(d2.checksum()));
        assertThat(d1.hashCode(), equalTo(d2.hashCode()));
    }

    private static CompressedData compressJson(String json) throws IOException {
        return new CompressedData(json.getBytes(StandardCharsets.UTF_8));
    }

    public void testSerialization() throws IOException {
        CompressedData before = compressJson("{\"test\":\"serialization\"}");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
        CompressedData after = new CompressedData(in);

        assertEquals(before, after);
    }

    public void testConstructorAndGetters() throws IOException {
        CompressedData data = compressJson("{\"key\":\"value\"}");
        assertNotNull(data.compressedBytes());
        assertNotNull(data.compressedReference());
        assertTrue(data.compressedBytes().length > 0);
    }

    public void testDefensiveCopy() throws IOException {
        CompressedData data = compressJson("{\"key\":\"value\"}");
        byte[] bytes1 = data.compressedBytes();
        byte[] bytes2 = data.compressedBytes();
        assertArrayEquals(bytes1, bytes2);
        assertNotSame(bytes1, bytes2);
    }

    public void testUncompressedRoundTrip() throws IOException {
        String json = "{\"field\":\"round-trip\"}";
        CompressedData data = compressJson(json);

        byte[] uncompressed = data.uncompressed();
        assertThat(new String(uncompressed, StandardCharsets.UTF_8), equalTo(json));

        CRC32 crc32 = new CRC32();
        crc32.update(json.getBytes(StandardCharsets.UTF_8));
        assertThat(data.checksum(), equalTo((int) crc32.getValue()));
    }

    public void testEqualsIdenticalBytes() throws IOException {
        CompressedData d1 = compressJson("{\"a\":1}");
        CompressedData d2 = new CompressedData(d1.compressedBytes(), d1.checksum());
        assertEquals(d1, d2);
    }

    public void testEqualsSameContentDifferentCompression() throws IOException {
        // Same uncompressed content but constructed separately — may have same compressed bytes
        // but tests the equals path
        String json = "{\"same\":\"content\"}";
        CompressedData d1 = compressJson(json);
        CompressedData d2 = compressJson(json);
        assertEquals(d1, d2);
    }

    public void testNotEqualsDifferentContent() throws IOException {
        CompressedData d1 = compressJson("{\"a\":1}");
        CompressedData d2 = compressJson("{\"b\":2}");
        assertThat(d1, not(equalTo(d2)));
    }

    public void testEqualsFastPathSameBytes() {
        // Smart equals: if compressed bytes match, they're equal (fast path)
        byte[] bytes = new byte[] { 1, 2, 3 };
        CompressedData d1 = new CompressedData(bytes, 100);
        CompressedData d2 = new CompressedData(bytes.clone(), 100);
        assertThat(d1, equalTo(d2));
    }

    public void testNotEqualsDifferentBytesAndChecksum() {
        CompressedData d1 = new CompressedData(new byte[] { 1, 2, 3 }, 100);
        CompressedData d2 = new CompressedData(new byte[] { 4, 5, 6 }, 200);
        assertThat(d1, not(equalTo(d2)));
    }

    public void testHashCode() throws IOException {
        CompressedData data = compressJson("{\"hash\":\"test\"}");
        assertThat(data.hashCode(), equalTo(data.checksum()));
    }

    public void testToString() throws IOException {
        String json = "{\"to\":\"string\"}";
        CompressedData data = compressJson(json);
        assertThat(data.toString(), equalTo(json));
    }

    public void testAutoDetectConstructor() throws IOException {
        String json = "{\"from\":\"bytes\"}";
        byte[] raw = json.getBytes(StandardCharsets.UTF_8);

        // constructor with uncompressed input
        CompressedData fromRaw = new CompressedData(raw);
        assertThat(new String(fromRaw.uncompressed(), StandardCharsets.UTF_8), equalTo(json));

        // constructor with already-compressed input
        CompressedData fromCompressed = new CompressedData(fromRaw.compressedBytes());
        assertThat(new String(fromCompressed.uncompressed(), StandardCharsets.UTF_8), equalTo(json));
        assertThat(fromRaw.checksum(), equalTo(fromCompressed.checksum()));
    }

    public void testAutoDetectConstructorNullThrows() {
        expectThrows(NullPointerException.class, () -> new CompressedData((byte[]) null));
    }

    public void testRawConstructorNullThrows() {
        expectThrows(NullPointerException.class, () -> new CompressedData(null, 0));
    }

    public void testSerializationRandom() throws IOException {
        for (int i = 0; i < 50; i++) {
            String json = "{\"i\":" + i + ",\"val\":\"" + randomAlphaOfLength(20) + "\"}";
            CompressedData original = compressJson(json);

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            StreamOutput out = new OutputStreamStreamOutput(baos);
            original.writeTo(out);
            out.close();

            StreamInput in = new InputStreamStreamInput(new ByteArrayInputStream(baos.toByteArray()));
            CompressedData deserialized = new CompressedData(in);

            assertEquals(original, deserialized);
            assertThat(new String(deserialized.uncompressed(), StandardCharsets.UTF_8), equalTo(json));
        }
    }

    public void testEqualsNull() throws IOException {
        CompressedData data = compressJson("{\"null\":\"test\"}");
        assertNotEquals(data, null);
    }

    public void testEqualsDifferentType() throws IOException {
        CompressedData data = compressJson("{\"type\":\"test\"}");
        assertNotEquals(data, "not a CompressedData");
    }
}
