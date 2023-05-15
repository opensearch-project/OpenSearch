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

package org.opensearch.repositories.s3;

import org.mockito.ArgumentCaptor;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.test.OpenSearchTestCase;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3BlobStoreContainerTests extends OpenSearchTestCase {

    public void testExecuteSingleUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.GB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeSingleUpload(blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize)
        );
        assertEquals("Upload request size [" + blobSize + "] can't be larger than 5gb", e.getMessage());
    }

    public void testExecuteSingleUploadBlobSizeLargerThanBufferSize() {
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bufferSizeInBytes()).thenReturn(ByteSizeUnit.MB.toBytes(1));

        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeSingleUpload(blobStore, blobName, new ByteArrayInputStream(new byte[0]), ByteSizeUnit.MB.toBytes(2))
        );
        assertEquals("Upload request size [2097152] can't be larger than buffer size", e.getMessage());
    }

    public void testExecuteSingleUpload() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();
        if (randomBoolean()) {
            IntStream.of(randomIntBetween(1, 5)).forEach(value -> blobPath.add("path_" + value));
        }

        final int bufferSize = randomIntBetween(1024, 2048);
        final int blobSize = randomIntBetween(0, bufferSize);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn((long) bufferSize);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        final boolean serverSideEncryption = randomBoolean();
        when(blobStore.serverSideEncryption()).thenReturn(serverSideEncryption);

        final StorageClass storageClass = randomFrom(StorageClass.values());
        when(blobStore.getStorageClass()).thenReturn(storageClass);

        final ObjectCannedACL cannedAccessControlList = randomBoolean() ? randomFrom(ObjectCannedACL.values()) : null;
        if (cannedAccessControlList != null) {
            when(blobStore.getCannedACL()).thenReturn(cannedAccessControlList);
        }

        final S3Client client = mock(S3Client.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        when(blobStore.clientReference()).thenReturn(clientReference);

        final ArgumentCaptor<PutObjectRequest> putObjectRequestArgumentCaptor = ArgumentCaptor.forClass(PutObjectRequest.class);
        final ArgumentCaptor<RequestBody> requestBodyArgumentCaptor = ArgumentCaptor.forClass(RequestBody.class);
        when(client.putObject(putObjectRequestArgumentCaptor.capture(), requestBodyArgumentCaptor.capture())).thenReturn(
            PutObjectResponse.builder().build()
        );

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[blobSize]);
        blobContainer.executeSingleUpload(blobStore, blobName, inputStream, blobSize);

        final PutObjectRequest request = putObjectRequestArgumentCaptor.getValue();
        final RequestBody requestBody = requestBodyArgumentCaptor.getValue();
        assertEquals(bucketName, request.bucket());
        assertEquals(blobPath.buildAsString() + blobName, request.key());
        byte[] expectedBytes = inputStream.readAllBytes();
        try (InputStream is = requestBody.contentStreamProvider().newStream()) {
            assertArrayEquals(expectedBytes, is.readAllBytes());
        }
        assertEquals(blobSize, request.contentLength().longValue());
        assertEquals(storageClass, request.storageClass());
        assertEquals(cannedAccessControlList, request.acl());
        if (serverSideEncryption) {
            assertEquals(ServerSideEncryption.AES256.toString(), request.sseCustomerAlgorithm());
        }
    }

    public void testExecuteMultipartUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.TB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeMultipartUpload(blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize)
        );
        assertEquals("Multipart upload request size [" + blobSize + "] can't be larger than 5tb", e.getMessage());
    }

    public void testExecuteMultipartUploadBlobSizeTooSmall() {
        final long blobSize = ByteSizeUnit.MB.toBytes(randomIntBetween(1, 4));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeMultipartUpload(blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize)
        );
        assertEquals("Multipart upload request size [" + blobSize + "] can't be smaller than 5mb", e.getMessage());
    }

    public void testExecuteMultipartUpload() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();
        if (randomBoolean()) {
            IntStream.of(randomIntBetween(1, 5)).forEach(value -> blobPath.add("path_" + value));
        }

        final long blobSize = ByteSizeUnit.GB.toBytes(randomIntBetween(1, 128));
        final long bufferSize = ByteSizeUnit.MB.toBytes(randomIntBetween(5, 1024));

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);

        final boolean serverSideEncryption = randomBoolean();
        when(blobStore.serverSideEncryption()).thenReturn(serverSideEncryption);

        final StorageClass storageClass = randomFrom(StorageClass.values());
        when(blobStore.getStorageClass()).thenReturn(storageClass);

        final ObjectCannedACL cannedAccessControlList = randomBoolean() ? randomFrom(ObjectCannedACL.values()) : null;
        if (cannedAccessControlList != null) {
            when(blobStore.getCannedACL()).thenReturn(cannedAccessControlList);
        }

        final S3Client client = mock(S3Client.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        when(blobStore.clientReference()).thenReturn(clientReference);

        final ArgumentCaptor<CreateMultipartUploadRequest> createMultipartUploadRequestArgumentCaptor = ArgumentCaptor.forClass(
            CreateMultipartUploadRequest.class
        );
        final CreateMultipartUploadResponse createMultipartUploadResponse = CreateMultipartUploadResponse.builder()
            .uploadId(randomAlphaOfLength(10))
            .build();
        when(client.createMultipartUpload(createMultipartUploadRequestArgumentCaptor.capture())).thenReturn(createMultipartUploadResponse);

        final ArgumentCaptor<UploadPartRequest> uploadPartRequestArgumentCaptor = ArgumentCaptor.forClass(UploadPartRequest.class);
        final ArgumentCaptor<RequestBody> requestBodyArgumentCaptor = ArgumentCaptor.forClass(RequestBody.class);

        final List<String> expectedEtags = new ArrayList<>();
        final long partSize = Math.min(bufferSize, blobSize);
        long totalBytes = 0;
        do {
            expectedEtags.add(randomAlphaOfLength(50));
            totalBytes += partSize;
        } while (totalBytes < blobSize);

        when(client.uploadPart(uploadPartRequestArgumentCaptor.capture(), requestBodyArgumentCaptor.capture())).thenAnswer(
            invocationOnMock -> {
                final UploadPartRequest request = (UploadPartRequest) invocationOnMock.getArguments()[0];
                final UploadPartResponse response = UploadPartResponse.builder().eTag(expectedEtags.get(request.partNumber() - 1)).build();
                // TODO do we need to set part number here?
                // response.setPartNumber(request.getPartNumber());
                return response;
            }
        );

        final ArgumentCaptor<CompleteMultipartUploadRequest> completeMultipartUploadRequestArgumentCaptor = ArgumentCaptor.forClass(
            CompleteMultipartUploadRequest.class
        );
        when(client.completeMultipartUpload(completeMultipartUploadRequestArgumentCaptor.capture())).thenReturn(
            CompleteMultipartUploadResponse.builder().build()
        );

        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
        blobContainer.executeMultipartUpload(blobStore, blobName, inputStream, blobSize);

        final CreateMultipartUploadRequest initRequest = createMultipartUploadRequestArgumentCaptor.getValue();
        assertEquals(bucketName, initRequest.bucket());
        assertEquals(blobPath.buildAsString() + blobName, initRequest.key());
        assertEquals(storageClass, initRequest.storageClass());
        assertEquals(cannedAccessControlList, initRequest.acl());
        if (serverSideEncryption) {
            assertEquals(ServerSideEncryption.AES256.toString(), initRequest.sseCustomerAlgorithm());
        }

        final Tuple<Long, Long> numberOfParts = S3BlobContainer.numberOfMultiparts(blobSize, bufferSize);

        final List<UploadPartRequest> uploadRequests = uploadPartRequestArgumentCaptor.getAllValues();
        final List<RequestBody> requestBodies = requestBodyArgumentCaptor.getAllValues();
        assertEquals(numberOfParts.v1().intValue(), uploadRequests.size());

        for (int i = 0; i < uploadRequests.size(); i++) {
            final UploadPartRequest uploadPartRequest = uploadRequests.get(i);
            final RequestBody requestBody = requestBodies.get(i);

            assertEquals(bucketName, uploadPartRequest.bucket());
            assertEquals(blobPath.buildAsString() + blobName, uploadPartRequest.key());
            assertEquals(createMultipartUploadResponse.uploadId(), uploadPartRequest.uploadId());
            assertEquals(i + 1, uploadPartRequest.partNumber().intValue());
            byte[] expectedBytes = inputStream.readAllBytes();
            try (InputStream is = requestBody.contentStreamProvider().newStream()) {
                byte[] actualBytes = is.readAllBytes();
                assertArrayEquals(expectedBytes, actualBytes);
            }
        }

        final CompleteMultipartUploadRequest completeMultipartUploadRequest = completeMultipartUploadRequestArgumentCaptor.getValue();
        assertEquals(bucketName, completeMultipartUploadRequest.bucket());
        assertEquals(blobPath.buildAsString() + blobName, completeMultipartUploadRequest.key());
        assertEquals(createMultipartUploadResponse.uploadId(), completeMultipartUploadRequest.uploadId());

        final List<String> actualETags = completeMultipartUploadRequest.multipartUpload()
            .parts()
            .stream()
            .map(CompletedPart::eTag)
            .collect(Collectors.toList());
        assertEquals(expectedEtags, actualETags);
    }

    public void testExecuteMultipartUploadAborted() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final long blobSize = ByteSizeUnit.MB.toBytes(765);
        final long bufferSize = ByteSizeUnit.MB.toBytes(150);

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.bufferSizeInBytes()).thenReturn(bufferSize);
        when(blobStore.getStorageClass()).thenReturn(randomFrom(StorageClass.values()));
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());

        final S3Client client = mock(S3Client.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        doAnswer(invocation -> {
            clientReference.incRef();
            return clientReference;
        }).when(blobStore).clientReference();

        final String uploadId = randomAlphaOfLength(25);

        final int stage = randomInt(2);
        final List<SdkException> exceptions = Arrays.asList(
            // TODO need to review exception initialization here
            SdkException.create("Expected initialization request to fail", new RuntimeException()),
            SdkException.create("Expected upload part request to fail", new RuntimeException()),
            SdkException.create("Expected completion request to fail", new RuntimeException())
        );

        if (stage == 0) {
            // Fail the initialization request
            when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenThrow(exceptions.get(stage));

        } else if (stage == 1) {
            final CreateMultipartUploadResponse createMultipartUploadResponse = CreateMultipartUploadResponse.builder()
                .uploadId(uploadId)
                .build();
            when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(createMultipartUploadResponse);

            // Fail the upload part request
            when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenThrow(exceptions.get(stage));

        } else {
            final CreateMultipartUploadResponse createMultipartUploadResponse = CreateMultipartUploadResponse.builder()
                .uploadId(uploadId)
                .build();
            when(client.createMultipartUpload(any(CreateMultipartUploadRequest.class))).thenReturn(createMultipartUploadResponse);

            when(client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class))).thenAnswer(invocationOnMock -> {
                final UploadPartRequest request = (UploadPartRequest) invocationOnMock.getArguments()[0];
                final UploadPartResponse response = UploadPartResponse.builder().eTag(randomAlphaOfLength(20)).build();
                // TODO review this
                // response.setPartNumber(request.getPartNumber());
                return response;
            });

            // Fail the completion request
            when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(exceptions.get(stage));
        }

        final ArgumentCaptor<AbortMultipartUploadRequest> argumentCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        when(client.abortMultipartUpload(argumentCaptor.capture())).thenReturn(AbortMultipartUploadResponse.builder().build());

        final IOException e = expectThrows(IOException.class, () -> {
            final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
            blobContainer.executeMultipartUpload(blobStore, blobName, new ByteArrayInputStream(new byte[0]), blobSize);
        });

        assertEquals("Unable to upload object [" + blobName + "] using multipart upload", e.getMessage());
        assertThat(e.getCause(), instanceOf(SdkException.class));
        assertEquals(exceptions.get(stage).getMessage(), e.getCause().getMessage());

        if (stage == 0) {
            verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));
            verify(client, times(0)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
            verify(client, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            verify(client, times(0)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

        } else {
            verify(client, times(1)).createMultipartUpload(any(CreateMultipartUploadRequest.class));

            if (stage == 1) {
                verify(client, times(1)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
                verify(client, times(0)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            } else {
                verify(client, times(6)).uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
                verify(client, times(1)).completeMultipartUpload(any(CompleteMultipartUploadRequest.class));
            }

            verify(client, times(1)).abortMultipartUpload(any(AbortMultipartUploadRequest.class));

            final AbortMultipartUploadRequest abortRequest = argumentCaptor.getValue();
            assertEquals(bucketName, abortRequest.bucket());
            assertEquals(blobName, abortRequest.key());
            assertEquals(uploadId, abortRequest.uploadId());
        }
    }

    public void testNumberOfMultipartsWithZeroPartSize() {
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> S3BlobContainer.numberOfMultiparts(randomNonNegativeLong(), 0L)
        );
        assertEquals("Part size must be greater than zero", e.getMessage());
    }

    public void testNumberOfMultiparts() {
        final ByteSizeUnit unit = randomFrom(ByteSizeUnit.BYTES, ByteSizeUnit.KB, ByteSizeUnit.MB, ByteSizeUnit.GB);
        final long size = unit.toBytes(randomIntBetween(2, 1000));
        final int factor = randomIntBetween(2, 10);

        // Fits in 1 empty part
        assertNumberOfMultiparts(1, 0L, 0L, size);

        // Fits in 1 part exactly
        assertNumberOfMultiparts(1, size, size, size);
        assertNumberOfMultiparts(1, size, size, size * factor);

        // Fits in N parts exactly
        assertNumberOfMultiparts(factor, size, size * factor, size);

        // Fits in N parts plus a bit more
        final long remaining = randomIntBetween(1, (size > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) size - 1);
        assertNumberOfMultiparts(factor + 1, remaining, (size * factor) + remaining, size);
    }

    public void testInitCannedACL() {
        String[] aclList = new String[] {
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "bucket-owner-read",
            "bucket-owner-full-control" };

        // empty acl
        assertThat(S3BlobStore.initCannedACL(null), equalTo(ObjectCannedACL.PRIVATE));
        assertThat(S3BlobStore.initCannedACL(""), equalTo(ObjectCannedACL.PRIVATE));

        // it should init cannedACL correctly
        for (String aclString : aclList) {
            ObjectCannedACL acl = S3BlobStore.initCannedACL(aclString);
            assertThat(acl.toString(), equalTo(aclString));
        }

        // it should accept all aws cannedACLs
        for (ObjectCannedACL awsList : ObjectCannedACL.values()) {
            ObjectCannedACL acl = S3BlobStore.initCannedACL(awsList.toString());
            assertThat(acl, equalTo(awsList));
        }
    }

    public void testInvalidCannedACL() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initCannedACL("test_invalid"));
        assertThat(ex.getMessage(), equalTo("cannedACL is not valid: [test_invalid]"));
    }

    public void testInitStorageClass() {
        // it should default to `standard`
        assertThat(S3BlobStore.initStorageClass(null), equalTo(StorageClass.STANDARD));
        assertThat(S3BlobStore.initStorageClass(""), equalTo(StorageClass.STANDARD));

        // it should accept [standard, standard_ia, onezone_ia, reduced_redundancy, intelligent_tiering]
        assertThat(S3BlobStore.initStorageClass("standard"), equalTo(StorageClass.STANDARD));
        assertThat(S3BlobStore.initStorageClass("standard_ia"), equalTo(StorageClass.STANDARD_IA));
        assertThat(S3BlobStore.initStorageClass("onezone_ia"), equalTo(StorageClass.ONEZONE_IA));
        assertThat(S3BlobStore.initStorageClass("reduced_redundancy"), equalTo(StorageClass.REDUCED_REDUNDANCY));
        assertThat(S3BlobStore.initStorageClass("intelligent_tiering"), equalTo(StorageClass.INTELLIGENT_TIERING));
    }

    public void testCaseInsensitiveStorageClass() {
        assertThat(S3BlobStore.initStorageClass("sTandaRd"), equalTo(StorageClass.STANDARD));
        assertThat(S3BlobStore.initStorageClass("sTandaRd_Ia"), equalTo(StorageClass.STANDARD_IA));
        assertThat(S3BlobStore.initStorageClass("oNeZoNe_iA"), equalTo(StorageClass.ONEZONE_IA));
        assertThat(S3BlobStore.initStorageClass("reduCED_redundancy"), equalTo(StorageClass.REDUCED_REDUNDANCY));
        assertThat(S3BlobStore.initStorageClass("intelLigeNt_tieriNG"), equalTo(StorageClass.INTELLIGENT_TIERING));
    }

    public void testInvalidStorageClass() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initStorageClass("whatever"));
        assertThat(ex.getMessage(), equalTo("`whatever` is not a valid S3 Storage Class."));
    }

    public void testRejectGlacierStorageClass() {
        BlobStoreException ex = expectThrows(BlobStoreException.class, () -> S3BlobStore.initStorageClass("glacier"));
        assertThat(ex.getMessage(), equalTo("Glacier storage class is not supported"));
    }

    private static void assertNumberOfMultiparts(final int expectedParts, final long expectedRemaining, long totalSize, long partSize) {
        final Tuple<Long, Long> result = S3BlobContainer.numberOfMultiparts(totalSize, partSize);

        assertEquals("Expected number of parts [" + expectedParts + "] but got [" + result.v1() + "]", expectedParts, (long) result.v1());
        assertEquals("Expected remaining [" + expectedRemaining + "] but got [" + result.v2() + "]", expectedRemaining, (long) result.v2());
    }
}
