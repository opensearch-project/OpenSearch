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

package org.opensearch.common.compress;

import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.compress.Compressor;
import org.opensearch.metadata.compress.CompressedData;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Random;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class DeflateCompressedXContentTests extends OpenSearchTestCase {

    private final Compressor compressor = new DeflateCompressor();

    private void assertEquals(CompressedXContent s1, CompressedXContent s2) {
        Assert.assertEquals(s1, s2);
        assertEquals(s1.uncompressed(), s2.uncompressed());
        assertEquals(s1.hashCode(), s2.hashCode());
    }

    public void simpleTests() throws IOException {
        String str = "---\nf:this is a simple string";
        CompressedXContent cstr = new CompressedXContent(str);
        assertThat(cstr.string(), equalTo(str));
        assertThat(new CompressedXContent(str), equalTo(cstr));

        String str2 = "---\nf:this is a simple string 2";
        CompressedXContent cstr2 = new CompressedXContent(str2);
        assertThat(cstr2.string(), not(equalTo(str)));
        assertThat(new CompressedXContent(str2), not(equalTo(cstr)));
        assertEquals(new CompressedXContent(str2), cstr2);
    }

    public void testRandom() throws IOException {
        Random r = random();
        for (int i = 0; i < 1000; i++) {
            String string = TestUtil.randomUnicodeString(r, 10000);
            // hack to make it detected as YAML
            string = "---\n" + string;
            CompressedXContent compressedXContent = new CompressedXContent(string);
            assertThat(compressedXContent.string(), equalTo(string));
        }
    }

    public void testDifferentCompressedRepresentation() throws Exception {
        byte[] b = "---\nf:abcdefghijabcdefghij".getBytes("UTF-8");
        BytesStreamOutput bout = new BytesStreamOutput();
        try (OutputStream out = compressor.threadLocalOutputStream(bout)) {
            out.write(b);
            out.flush();
            out.write(b);
        }
        final BytesReference b1 = bout.bytes();

        bout = new BytesStreamOutput();
        try (OutputStream out = compressor.threadLocalOutputStream(bout)) {
            out.write(b);
            out.write(b);
        }
        final BytesReference b2 = bout.bytes();

        // because of the intermediate flush, the two compressed representations
        // are different. It can also happen for other reasons like if hash tables
        // of different size are being used
        assertFalse(b1.equals(b2));
        // we used the compressed representation directly and did not recompress
        assertArrayEquals(BytesReference.toBytes(b1), new CompressedXContent(b1).compressed());
        assertArrayEquals(BytesReference.toBytes(b2), new CompressedXContent(b2).compressed());
        // but compressedstring instances are still equal
        assertEquals(new CompressedXContent(b1), new CompressedXContent(b2));
    }

    public void testHashCode() throws IOException {
        assertFalse(new CompressedXContent("{\"a\":\"b\"}").hashCode() == new CompressedXContent("{\"a\":\"c\"}").hashCode());
    }

    public void testCompressedDataSerialization() throws IOException {
        String str = "---\nf:this is a test string for compressed data";
        CompressedXContent compressedXContent = new CompressedXContent(str);

        // Serialize using CompressedXContent
        final BytesStreamOutput out = new BytesStreamOutput();
        compressedXContent.writeTo(out);

        // Deserialize using CompressedData
        final StreamInput in = out.bytes().streamInput();
        final CompressedData compressedData = new CompressedData(in);

        // Verify the data matches
        assertArrayEquals(compressedXContent.compressed(), compressedData.compressedBytes());
        assertThat(compressedXContent.compressedData().checksum(), equalTo(compressedData.checksum()));
    }

    public void testCompressedDataToCompressedXContent() throws IOException {
        String str = "---\nf:this is a test string for round trip";
        CompressedXContent original = new CompressedXContent(str);

        // Serialize using CompressedXContent
        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        // Deserialize as CompressedData
        final StreamInput in1 = out1.bytes().streamInput();
        final CompressedData compressedData = new CompressedData(in1);

        // Serialize using CompressedData
        final BytesStreamOutput out2 = new BytesStreamOutput();
        compressedData.writeTo(out2);

        // Deserialize as CompressedXContent
        final StreamInput in2 = out2.bytes().streamInput();
        final CompressedXContent restored = CompressedXContent.readCompressedString(in2);

        // Verify round-trip preserves data
        assertEquals(original, restored);
        assertThat(restored.string(), equalTo(str));
    }

    public void testCompressedDataRoundTripWithRandomContent() throws IOException {
        Random r = random();
        for (int i = 0; i < 100; i++) {
            String string = TestUtil.randomUnicodeString(r, 1000);
            // hack to make it detected as YAML
            string = "---\n" + string;

            CompressedXContent original = new CompressedXContent(string);

            // CompressedXContent -> CompressedData -> CompressedXContent
            final BytesStreamOutput out1 = new BytesStreamOutput();
            original.writeTo(out1);

            final StreamInput in1 = out1.bytes().streamInput();
            final CompressedData compressedData = new CompressedData(in1);

            final BytesStreamOutput out2 = new BytesStreamOutput();
            compressedData.writeTo(out2);

            final StreamInput in2 = out2.bytes().streamInput();
            final CompressedXContent restored = CompressedXContent.readCompressedString(in2);

            assertEquals(original, restored);
            assertThat(restored.string(), equalTo(string));
        }
    }

    public void testCompressedXContentFromCompressedData() throws IOException {
        String str = "---\nf:test content";
        CompressedXContent original = new CompressedXContent(str);

        // Get CompressedData from CompressedXContent
        CompressedData compressedData = original.compressedData();

        // Create new CompressedXContent from CompressedData
        CompressedXContent fromData = new CompressedXContent(compressedData);

        // Verify they are equal
        assertEquals(original, fromData);
        assertThat(fromData.string(), equalTo(str));
    }

}
