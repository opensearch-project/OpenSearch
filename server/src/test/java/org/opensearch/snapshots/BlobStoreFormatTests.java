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
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStore;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.fs.FsBlobContainer;
import org.opensearch.common.blobstore.fs.FsBlobStore;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.io.Streams;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.compress.CompressorRegistry;
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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.mockito.ArgumentCaptor;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

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

    public void testBlobStoreAsyncOperations() throws IOException, InterruptedException {
        BlobStore blobStore = createTestBlobStore();
        MockFsVerifyingBlobContainer mockBlobContainer = new MockFsVerifyingBlobContainer(
            (FsBlobStore) blobStore,
            BlobPath.cleanPath(),
            null
        );
        MockFsVerifyingBlobContainer spyContainer = spy(mockBlobContainer);
        ChecksumBlobStoreFormat<BlobObj> checksumSMILE = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent);
        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);
        ArgumentCaptor<WriteContext> writeContextArgumentCaptor = ArgumentCaptor.forClass(WriteContext.class);
        CountDownLatch latch = new CountDownLatch(2);

        // Write blobs in different formats
        checksumSMILE.writeAsync(
            new BlobObj("checksum smile"),
            spyContainer,
            "check-smile",
            CompressorRegistry.none(),
            getVoidActionListener(latch),
            ChecksumBlobStoreFormat.SNAPSHOT_ONLY_FORMAT_PARAMS
        );
        checksumSMILE.writeAsync(
            new BlobObj("checksum smile compressed"),
            spyContainer,
            "check-smile-comp",
            CompressorRegistry.getCompressor(DeflateCompressor.NAME),
            getVoidActionListener(latch),
            ChecksumBlobStoreFormat.SNAPSHOT_ONLY_FORMAT_PARAMS
        );

        latch.await();

        verify(spyContainer, times(2)).asyncBlobUpload(writeContextArgumentCaptor.capture(), actionListenerArgumentCaptor.capture());
        assertEquals(2, writeContextArgumentCaptor.getAllValues().size());
        writeContextArgumentCaptor.getAllValues()
            .forEach(writeContext -> assertEquals(WritePriority.NORMAL, writeContext.getWritePriority()));
        // Assert that all checksum blobs can be read
        assertEquals(checksumSMILE.read(mockBlobContainer.getDelegate(), "check-smile", xContentRegistry()).getText(), "checksum smile");
        assertEquals(
            checksumSMILE.read(mockBlobContainer.getDelegate(), "check-smile-comp", xContentRegistry()).getText(),
            "checksum smile compressed"
        );
    }

    public void testBlobStorePriorityAsyncOperation() throws IOException, InterruptedException {
        BlobStore blobStore = createTestBlobStore();
        MockFsVerifyingBlobContainer mockBlobContainer = new MockFsVerifyingBlobContainer(
            (FsBlobStore) blobStore,
            BlobPath.cleanPath(),
            null
        );
        MockFsVerifyingBlobContainer spyContainer = spy(mockBlobContainer);
        ChecksumBlobStoreFormat<BlobObj> checksumSMILE = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent);

        ArgumentCaptor<ActionListener<Void>> actionListenerArgumentCaptor = ArgumentCaptor.forClass(ActionListener.class);
        ArgumentCaptor<WriteContext> writeContextArgumentCaptor = ArgumentCaptor.forClass(WriteContext.class);
        CountDownLatch latch = new CountDownLatch(1);

        // Write blobs in different formats
        checksumSMILE.writeAsyncWithUrgentPriority(
            new BlobObj("cluster state diff"),
            spyContainer,
            "cluster-state-diff",
            CompressorRegistry.none(),
            getVoidActionListener(latch),
            ChecksumBlobStoreFormat.SNAPSHOT_ONLY_FORMAT_PARAMS
        );
        latch.await();

        verify(spyContainer).asyncBlobUpload(writeContextArgumentCaptor.capture(), actionListenerArgumentCaptor.capture());
        assertEquals(WritePriority.URGENT, writeContextArgumentCaptor.getValue().getWritePriority());
        assertEquals(
            checksumSMILE.read(mockBlobContainer.getDelegate(), "cluster-state-diff", xContentRegistry()).getText(),
            "cluster state diff"
        );
    }

    public void testBlobStoreOperations() throws IOException {
        BlobStore blobStore = createTestBlobStore();
        BlobContainer blobContainer = blobStore.blobContainer(BlobPath.cleanPath());
        ChecksumBlobStoreFormat<BlobObj> checksumSMILE = new ChecksumBlobStoreFormat<>(BLOB_CODEC, "%s", BlobObj::fromXContent);

        // Write blobs in different formats
        checksumSMILE.write(new BlobObj("checksum smile"), blobContainer, "check-smile", CompressorRegistry.none());
        checksumSMILE.write(
            new BlobObj("checksum smile compressed"),
            blobContainer,
            "check-smile-comp",
            CompressorRegistry.getCompressor(DeflateCompressor.NAME)
        );

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
        checksumFormat.write(blobObj, blobContainer, "blob-comp", CompressorRegistry.getCompressor(DeflateCompressor.NAME));
        checksumFormat.write(blobObj, blobContainer, "blob-not-comp", CompressorRegistry.none());
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
        checksumFormat.write(blobObj, blobContainer, "test-path", randomFrom(CompressorRegistry.registeredCompressors().values()));
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

    private ActionListener<Void> getVoidActionListener(CountDownLatch latch) {
        ActionListener<Void> actionListener = new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.info("---> Async write succeeded");
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.info("---> Failure in async write");
                throw new RuntimeException("async write should not fail");
            }
        };

        return actionListener;
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

    public static class MockFsVerifyingBlobContainer extends FsBlobContainer implements AsyncMultiStreamBlobContainer {

        private BlobContainer delegate;

        public MockFsVerifyingBlobContainer(FsBlobStore blobStore, BlobPath blobPath, Path path) {
            super(blobStore, blobPath, path);
            delegate = blobStore.blobContainer(BlobPath.cleanPath());
        }

        @Override
        public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
            InputStream inputStream = writeContext.getStreamProvider(Integer.MAX_VALUE).provideStream(0).getInputStream();
            delegate.writeBlob(writeContext.getFileName(), inputStream, writeContext.getFileSize(), true);
            completionListener.onResponse(null);
        }

        @Override
        public void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
            throw new RuntimeException("read not supported");
        }

        @Override
        public boolean remoteIntegrityCheckSupported() {
            return false;
        }

        public BlobContainer getDelegate() {
            return delegate;
        }

        @Override
        public void deleteAsync(ActionListener<DeleteResult> completionListener) {
            throw new RuntimeException("deleteAsync not supported");
        }

        @Override
        public void deleteBlobsAsyncIgnoringIfNotExists(List<String> blobNames, ActionListener<Void> completionListener) {
            throw new RuntimeException("deleteBlobsAsyncIgnoringIfNotExists not supported");
        }
    }
}
