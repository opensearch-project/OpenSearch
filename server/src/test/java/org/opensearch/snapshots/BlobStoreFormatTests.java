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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.snapshots;

import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.OpenSearchParseException;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.io.Streams;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.translog.BufferedChecksumStreamOutput;
import org.opensearch.repositories.blobstore.ChecksumBlobStoreFormat;
import org.opensearch.test.OpenSearchTestCase;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

public class BlobStoreFormatTests extends OpenSearchTestCase {

    public static final String BLOB_CODEC = "blob";

    private static class BlobObj implements ToXContentFragment {

        private final String text;

        BlobObj(String text) {
            this.text = text;
        }

        public String getText() {
            return text;
        }

        public static BlobObj fromXContent(XContentParser parser) throws IOException {
            String text = null;
            XContentParser.Token token = parser.currentToken();
            if (token == null) {
                token = parser.nextToken();
            }
            if (token == XContentParser.Token.START_OBJECT) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token != XContentParser.Token.FIELD_NAME) {
                        throw new OpenSearchParseException("unexpected token [{}]", token);
                    }
                    String currentFieldName = parser.currentName();
                    token = parser.nextToken();
                    if (token.isValue()) {
                        if ("text".equals(currentFieldName)) {
                            text = parser.text();
                        } else {
                            throw new OpenSearchParseException("unexpected field [{}]", currentFieldName);
                        }
                    } else {
                        throw new OpenSearchParseException("unexpected token [{}]", token);
                    }
                }
            }
            if (text == null) {
                throw new OpenSearchParseException("missing mandatory parameter text");
            }
            return new BlobObj(text);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.field("text", getText());
            return builder;
        }
    }

    public void testBlobStoreOperations() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        ChecksumBlobStoreFormat<BlobObj> checksumSMILE = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent);

        // Write blobs in different formats
        checksumSMILE.write(new BlobObj("checksum smile"), blobContainer, "check-smile", false);
        checksumSMILE.write(new BlobObj("checksum smile compressed"), blobContainer, "check-smile-comp", true);

        // Assert that all checksum blobs can be read
        assertEquals(checksumSMILE.read(blobContainer, "check-smile", xContentRegistry()).getText(), "checksum smile");
        assertEquals(checksumSMILE.read(blobContainer, "check-smile-comp", xContentRegistry()).getText(), "checksum smile compressed");
    }

    public void testCompressionIsApplied() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        StringBuilder veryRedundantText = new StringBuilder();
        for (int i = 0; i < randomIntBetween(100, 300); i++) {
            veryRedundantText.append("Blah ");
        }
        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent);
        BlobObj blobObj = new BlobObj(veryRedundantText.toString());
        checksumFormat.write(blobObj, blobContainer, "blob-comp", true);
        checksumFormat.write(blobObj, blobContainer, "blob-not-comp", false);
        Map<String, BlobMetadata> blobs = blobContainer.listBlobsByPrefix("blob-");
        assertEquals(blobs.size(), 2);
        assertThat(blobs.get("blob-not-comp").length(), greaterThan(blobs.get("blob-comp").length()));
    }

    public void testBlobCorruption() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        String testString = randomAlphaOfLength(randomInt(10000));
        BlobObj blobObj = new BlobObj(testString);
        ChecksumBlobStoreFormat<BlobObj> checksumFormat = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent);
        checksumFormat.write(blobObj, blobContainer, "test-path", randomBoolean());
        assertEquals(checksumFormat.read(blobContainer, "test-path", xContentRegistry()).getText(), testString);
        randomCorruption(blobContainer, "test-path");
        try {
            checksumFormat.read(blobContainer, "test-path", xContentRegistry());
            fail("Should have failed due to corruption");
        } catch (OpenSearchCorruptionException ex) {
            assertThat(ex.getMessage(), containsString("test-path"));
        } catch (EOFException ex) {
            // This can happen if corrupt the byte length
        }
    }

    protected BlobStore createTestBlobStore() throws IOException {
        return new FsBlobStore(randomIntBetween(1, 8) * 1024, createTempDir(), false);
    }

    protected void randomCorruption(BlobContainer blobContainer, String blobName) throws IOException {
        byte[] buffer = new byte[(int) blobContainer.listBlobsByPrefix(blobName).get(blobName).length()];
        long originalChecksum = checksum(buffer);
        try (InputStream inputStream = blobContainer.readBlob(blobName)) {
            Streams.readFully(inputStream, buffer);
        }
        do {
            int location = randomIntBetween(0, buffer.length - 1);
            buffer[location] = (byte) (buffer[location] ^ 42);
        } while (originalChecksum == checksum(buffer));
        BytesArray bytesArray = new BytesArray(buffer);
        try (StreamInput stream = bytesArray.streamInput()) {
            blobContainer.writeBlob(blobName, stream, bytesArray.length(), false);
        }
    }

    private long checksum(byte[] buffer) throws IOException {
        try (BytesStreamOutput streamOutput = new BytesStreamOutput()) {
            try (BufferedChecksumStreamOutput checksumOutput = new BufferedChecksumStreamOutput(streamOutput)) {
                checksumOutput.write(buffer);
                return checksumOutput.getChecksum();
            }
        }
    }
}
