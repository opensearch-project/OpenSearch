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

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.Checksum;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesParts;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class S3BlobStoreContainerTests extends OpenSearchTestCase {

    public void testExecuteSingleUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.GB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeSingleUpload(blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize, null)
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
            () -> blobContainer.executeSingleUpload(
                blobStore,
                blobName,
                new ByteArrayInputStream(new byte[0]),
                ByteSizeUnit.MB.toBytes(2),
                null
            )
        );
        assertEquals("Upload request size [2097152] can't be larger than buffer size", e.getMessage());
    }

    public void testBlobExists() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);

        final S3Client client = mock(S3Client.class);
        when(client.headObject(any(HeadObjectRequest.class))).thenReturn(HeadObjectResponse.builder().build());
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        when(blobStore.clientReference()).thenReturn(clientReference);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        assertTrue(blobContainer.blobExists(blobName));
        verify(client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    public void testBlobExistsNoSuchKeyException() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);

        final S3Client client = mock(S3Client.class);
        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(NoSuchKeyException.builder().build());
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        when(blobStore.clientReference()).thenReturn(clientReference);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        assertFalse(blobContainer.blobExists(blobName));
        verify(client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    public void testBlobExistsRequestFailure() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);

        final S3Client client = mock(S3Client.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        when(blobStore.clientReference()).thenReturn(clientReference);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        when(client.headObject(any(HeadObjectRequest.class))).thenThrow(new RuntimeException());

        assertThrows(BlobStoreException.class, () -> blobContainer.blobExists(blobName));
        verify(client, times(1)).headObject(any(HeadObjectRequest.class));
    }

    private static class MockListObjectsV2ResponseIterator implements Iterator<ListObjectsV2Response> {

        private final int totalPageCount;
        private final int s3ObjectsPerPage;
        private final long s3ObjectSize;

        private final AtomicInteger currInvocationCount = new AtomicInteger();
        private final List<String> keysListed;
        private final boolean throwExceptionOnNextInvocation;

        public MockListObjectsV2ResponseIterator(int totalPageCount, int s3ObjectsPerPage, long s3ObjectSize) {
            this(totalPageCount, s3ObjectsPerPage, s3ObjectSize, "");
        }

        public MockListObjectsV2ResponseIterator(int totalPageCount, int s3ObjectsPerPage, long s3ObjectSize, String blobPath) {
            this(totalPageCount, s3ObjectsPerPage, s3ObjectSize, blobPath, false);
        }

        public MockListObjectsV2ResponseIterator(
            int totalPageCount,
            int s3ObjectsPerPage,
            long s3ObjectSize,
            String blobPath,
            boolean throwExceptionOnNextInvocation
        ) {
            this.totalPageCount = totalPageCount;
            this.s3ObjectsPerPage = s3ObjectsPerPage;
            this.s3ObjectSize = s3ObjectSize;
            this.throwExceptionOnNextInvocation = throwExceptionOnNextInvocation;
            keysListed = new ArrayList<>();
            for (int i = 0; i < totalPageCount * s3ObjectsPerPage; i++) {
                keysListed.add(blobPath + UUID.randomUUID().toString());
            }
            // S3 lists keys in lexicographic order
            keysListed.sort(String::compareTo);
        }

        @Override
        public boolean hasNext() {
            return currInvocationCount.get() < totalPageCount;
        }

        @Override
        public ListObjectsV2Response next() {
            if (throwExceptionOnNextInvocation) {
                throw SdkException.builder().build();
            }
            if (currInvocationCount.getAndIncrement() < totalPageCount) {
                List<S3Object> s3Objects = new ArrayList<>();
                for (int i = 0; i < s3ObjectsPerPage; i++) {
                    String s3ObjectKey = keysListed.get((currInvocationCount.get() - 1) * s3ObjectsPerPage + i);
                    s3Objects.add(S3Object.builder().key(s3ObjectKey).size(s3ObjectSize).build());
                }
                return ListObjectsV2Response.builder().contents(s3Objects).build();
            }
            throw new NoSuchElementException();
        }

        public List<String> getKeysListed() {
            return keysListed;
        }

        public int numberOfPagesFetched() {
            return currInvocationCount.get();
        }
    }

    public void testDelete() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();
        int bulkDeleteSize = 5;

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.getBulkDeletesSize()).thenReturn(bulkDeleteSize);

        final S3Client client = mock(S3Client.class);
        doAnswer(invocation -> new AmazonS3Reference(client)).when(blobStore).clientReference();

        ListObjectsV2Iterable listObjectsV2Iterable = mock(ListObjectsV2Iterable.class);
        final int totalPageCount = 3;
        final long s3ObjectSize = ByteSizeUnit.MB.toBytes(5);
        final int s3ObjectsPerPage = 5;
        MockListObjectsV2ResponseIterator listObjectsV2ResponseIterator = new MockListObjectsV2ResponseIterator(
            totalPageCount,
            s3ObjectsPerPage,
            s3ObjectSize
        );
        when(listObjectsV2Iterable.iterator()).thenReturn(listObjectsV2ResponseIterator);
        when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Iterable);

        final List<String> keysDeleted = new ArrayList<>();
        AtomicInteger deleteCount = new AtomicInteger();
        doAnswer(invocation -> {
            DeleteObjectsRequest deleteObjectsRequest = invocation.getArgument(0);
            deleteCount.getAndIncrement();
            logger.info("Object sizes are{}", deleteObjectsRequest.delete().objects().size());
            keysDeleted.addAll(deleteObjectsRequest.delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toList()));
            return DeleteObjectsResponse.builder().build();
        }).when(client).deleteObjects(any(DeleteObjectsRequest.class));

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        DeleteResult deleteResult = blobContainer.delete();
        assertEquals(s3ObjectSize * s3ObjectsPerPage * totalPageCount, deleteResult.bytesDeleted());
        assertEquals(s3ObjectsPerPage * totalPageCount, deleteResult.blobsDeleted());
        // keysDeleted will have blobPath also
        assertEquals(listObjectsV2ResponseIterator.getKeysListed().size(), keysDeleted.size() - 1);
        assertTrue(keysDeleted.contains(blobPath.buildAsString()));
        // keysDeleted will have blobPath also
        assertEquals((int) Math.ceil(((double) keysDeleted.size() + 1) / bulkDeleteSize), deleteCount.get());
        keysDeleted.remove(blobPath.buildAsString());
        assertEquals(new HashSet<>(listObjectsV2ResponseIterator.getKeysListed()), new HashSet<>(keysDeleted));
    }

    public void testDeleteItemLevelErrorsDuringDelete() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());

        final S3Client client = mock(S3Client.class);
        doAnswer(invocation -> new AmazonS3Reference(client)).when(blobStore).clientReference();

        ListObjectsV2Iterable listObjectsV2Iterable = mock(ListObjectsV2Iterable.class);
        final int totalPageCount = 3;
        final long s3ObjectSize = ByteSizeUnit.MB.toBytes(5);
        final int s3ObjectsPerPage = 5;
        MockListObjectsV2ResponseIterator listObjectsV2ResponseIterator = new MockListObjectsV2ResponseIterator(
            totalPageCount,
            s3ObjectsPerPage,
            s3ObjectSize
        );
        when(listObjectsV2Iterable.iterator()).thenReturn(listObjectsV2ResponseIterator);
        when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Iterable);

        final List<String> keysFailedDeletion = new ArrayList<>();
        doAnswer(invocation -> {
            DeleteObjectsRequest deleteObjectsRequest = invocation.getArgument(0);
            int i = 0;
            for (ObjectIdentifier objectIdentifier : deleteObjectsRequest.delete().objects()) {
                if (i % 2 == 0) {
                    keysFailedDeletion.add(objectIdentifier.key());
                }
                i++;
            }
            return DeleteObjectsResponse.builder()
                .errors(keysFailedDeletion.stream().map(key -> S3Error.builder().key(key).build()).collect(Collectors.toList()))
                .build();
        }).when(client).deleteObjects(any(DeleteObjectsRequest.class));

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        assertThrows(AssertionError.class, blobContainer::delete);
    }

    public void testDeleteSdkExceptionDuringListOperation() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());

        final S3Client client = mock(S3Client.class);
        doAnswer(invocation -> new AmazonS3Reference(client)).when(blobStore).clientReference();

        ListObjectsV2Iterable listObjectsV2Iterable = mock(ListObjectsV2Iterable.class);
        final int totalPageCount = 3;
        final long s3ObjectSize = ByteSizeUnit.MB.toBytes(5);
        final int s3ObjectsPerPage = 5;
        MockListObjectsV2ResponseIterator listObjectsV2ResponseIterator = new MockListObjectsV2ResponseIterator(
            totalPageCount,
            s3ObjectsPerPage,
            s3ObjectSize
        );
        when(listObjectsV2Iterable.iterator()).thenReturn(listObjectsV2ResponseIterator);
        when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Iterable);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        assertThrows(IOException.class, blobContainer::delete);
    }

    public void testDeleteSdkExceptionDuringDeleteOperation() {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);

        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());

        final S3Client client = mock(S3Client.class);
        doAnswer(invocation -> new AmazonS3Reference(client)).when(blobStore).clientReference();

        ListObjectsV2Iterable listObjectsV2Iterable = mock(ListObjectsV2Iterable.class);
        final int totalPageCount = 3;
        final long s3ObjectSize = ByteSizeUnit.MB.toBytes(5);
        final int s3ObjectsPerPage = 5;
        MockListObjectsV2ResponseIterator listObjectsV2ResponseIterator = new MockListObjectsV2ResponseIterator(
            totalPageCount,
            s3ObjectsPerPage,
            s3ObjectSize
        );
        when(listObjectsV2Iterable.iterator()).thenReturn(listObjectsV2ResponseIterator);
        when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Iterable);

        when(client.deleteObjects(any(DeleteObjectsRequest.class))).thenThrow(SdkException.builder().build());

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        assertThrows(IOException.class, blobContainer::delete);
    }

    public void testExecuteSingleUpload() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

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
        blobContainer.executeSingleUpload(blobStore, blobName, inputStream, blobSize, metadata);

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
        assertEquals(metadata, request.metadata());
        if (serverSideEncryption) {
            assertEquals(ServerSideEncryption.AES256, request.serverSideEncryption());
        }
    }

    public void testExecuteMultipartUploadBlobSizeTooLarge() {
        final long blobSize = ByteSizeUnit.TB.toBytes(randomIntBetween(6, 10));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeMultipartUpload(blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize, null)
        );
        assertEquals("Multipart upload request size [" + blobSize + "] can't be larger than 5tb", e.getMessage());
    }

    public void testExecuteMultipartUploadBlobSizeTooSmall() {
        final long blobSize = ByteSizeUnit.MB.toBytes(randomIntBetween(1, 4));
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final S3BlobContainer blobContainer = new S3BlobContainer(mock(BlobPath.class), blobStore);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> blobContainer.executeMultipartUpload(blobStore, randomAlphaOfLengthBetween(1, 10), null, blobSize, null)
        );
        assertEquals("Multipart upload request size [" + blobSize + "] can't be smaller than 5mb", e.getMessage());
    }

    public void testExecuteMultipartUpload() throws IOException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);

        final Map<String, String> metadata = new HashMap<>();
        metadata.put("key1", "value1");
        metadata.put("key2", "value2");

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
        blobContainer.executeMultipartUpload(blobStore, blobName, inputStream, blobSize, metadata);

        final CreateMultipartUploadRequest initRequest = createMultipartUploadRequestArgumentCaptor.getValue();
        assertEquals(bucketName, initRequest.bucket());
        assertEquals(blobPath.buildAsString() + blobName, initRequest.key());
        assertEquals(storageClass, initRequest.storageClass());
        assertEquals(cannedAccessControlList, initRequest.acl());
        assertEquals(metadata, initRequest.metadata());

        if (serverSideEncryption) {
            assertEquals(ServerSideEncryption.AES256, initRequest.serverSideEncryption());
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
                return response;
            });

            // Fail the completion request
            when(client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class))).thenThrow(exceptions.get(stage));
        }

        final ArgumentCaptor<AbortMultipartUploadRequest> argumentCaptor = ArgumentCaptor.forClass(AbortMultipartUploadRequest.class);
        when(client.abortMultipartUpload(argumentCaptor.capture())).thenReturn(AbortMultipartUploadResponse.builder().build());

        final IOException e = expectThrows(IOException.class, () -> {
            final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
            blobContainer.executeMultipartUpload(blobStore, blobName, new ByteArrayInputStream(new byte[0]), blobSize, null);
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

    public void testListBlobsByPrefix() throws IOException {
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());

        final S3Client client = mock(S3Client.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        when(blobStore.clientReference()).thenReturn(clientReference);

        BlobPath blobPath = mock(BlobPath.class);
        when(blobPath.buildAsString()).thenReturn("/dummy/path");
        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        final ListObjectsV2Iterable listObjectsV2Iterable = mock(ListObjectsV2Iterable.class);
        when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Iterable);

        MockListObjectsV2ResponseIterator iterator = new MockListObjectsV2ResponseIterator(2, 5, 100);
        when(listObjectsV2Iterable.iterator()).thenReturn(iterator);

        Map<String, BlobMetadata> listOfBlobs = blobContainer.listBlobsByPrefix(null);
        assertEquals(10, listOfBlobs.size());

        Set<String> keys = iterator.keysListed.stream()
            .map(s -> s.substring(blobPath.buildAsString().length()))
            .collect(Collectors.toSet());
        assertEquals(keys, listOfBlobs.keySet());
    }

    private void testListBlobsByPrefixInLexicographicOrder(
        int limit,
        int expectedNumberofPagesFetched,
        BlobContainer.BlobNameSortOrder blobNameSortOrder
    ) throws IOException {
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());

        final S3Client client = mock(S3Client.class);
        final AmazonS3Reference clientReference = new AmazonS3Reference(client);
        when(blobStore.clientReference()).thenReturn(clientReference);

        BlobPath blobPath = mock(BlobPath.class);
        when(blobPath.buildAsString()).thenReturn("/dummy/path");
        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        final ListObjectsV2Iterable listObjectsV2Iterable = mock(ListObjectsV2Iterable.class);
        when(client.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(listObjectsV2Iterable);

        final MockListObjectsV2ResponseIterator iterator = new MockListObjectsV2ResponseIterator(2, 5, 100, blobPath.buildAsString());
        when(listObjectsV2Iterable.iterator()).thenReturn(iterator);

        if (limit >= 0) {
            blobContainer.listBlobsByPrefixInSortedOrder(null, limit, blobNameSortOrder, new ActionListener<>() {
                @Override
                public void onResponse(List<BlobMetadata> blobMetadata) {
                    int actualLimit = Math.max(0, Math.min(limit, 10));
                    assertEquals(actualLimit, blobMetadata.size());

                    List<String> keys = iterator.keysListed.stream()
                        .map(s -> s.substring(blobPath.buildAsString().length()))
                        .collect(Collectors.toList());
                    Comparator<String> keysComparator = String::compareTo;
                    if (blobNameSortOrder != BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC) {
                        keysComparator = Collections.reverseOrder(String::compareTo);
                    }
                    keys.sort(keysComparator);
                    List<String> sortedKeys = keys.subList(0, actualLimit);
                    assertEquals(sortedKeys, blobMetadata.stream().map(BlobMetadata::name).collect(Collectors.toList()));
                    assertEquals(expectedNumberofPagesFetched, iterator.numberOfPagesFetched());
                }

                @Override
                public void onFailure(Exception e) {
                    fail("blobContainer.listBlobsByPrefixInLexicographicOrder failed with exception: " + e.getMessage());
                }
            });
        } else {
            assertThrows(
                IllegalArgumentException.class,
                () -> blobContainer.listBlobsByPrefixInSortedOrder(null, limit, blobNameSortOrder, new ActionListener<>() {
                    @Override
                    public void onResponse(List<BlobMetadata> blobMetadata) {}

                    @Override
                    public void onFailure(Exception e) {}
                })
            );
        }
    }

    public void testListBlobsByPrefixInLexicographicOrderWithNegativeLimit() throws IOException {
        testListBlobsByPrefixInLexicographicOrder(-5, 0, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    public void testListBlobsByPrefixInLexicographicOrderWithZeroLimit() throws IOException {
        testListBlobsByPrefixInLexicographicOrder(0, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    public void testListBlobsByPrefixInLexicographicOrderWithLimitLessThanPageSize() throws IOException {
        testListBlobsByPrefixInLexicographicOrder(2, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    /**
     * Test the boundary value at page size to ensure
     * unnecessary calls are not made to S3 by fetching the next page.
     * @throws IOException
     */
    public void testListBlobsByPrefixInLexicographicOrderWithLimitEqualToPageSize() throws IOException {
        testListBlobsByPrefixInLexicographicOrder(5, 1, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    public void testListBlobsByPrefixInLexicographicOrderWithLimitGreaterThanPageSize() throws IOException {
        testListBlobsByPrefixInLexicographicOrder(8, 2, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    public void testListBlobsByPrefixInLexicographicOrderWithLimitGreaterThanNumberOfRecords() throws IOException {
        testListBlobsByPrefixInLexicographicOrder(12, 2, BlobContainer.BlobNameSortOrder.LEXICOGRAPHIC);
    }

    public void testReadBlobAsyncMultiPart() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final String checksum = randomAlphaOfLength(10);

        final long objectSize = 100L;
        final int objectPartCount = 10;
        final int partSize = 10;

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference amazonAsyncS3Reference = new AmazonAsyncS3Reference(
            AmazonAsyncS3WithCredentials.create(s3AsyncClient, s3AsyncClient, s3AsyncClient, null)
        );

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final BlobPath blobPath = new BlobPath();

        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.serverSideEncryption()).thenReturn(false);
        when(blobStore.asyncClientReference()).thenReturn(amazonAsyncS3Reference);

        CompletableFuture<GetObjectAttributesResponse> getObjectAttributesResponseCompletableFuture = new CompletableFuture<>();
        getObjectAttributesResponseCompletableFuture.complete(
            GetObjectAttributesResponse.builder()
                .checksum(Checksum.builder().checksumCRC32(checksum).build())
                .objectSize(objectSize)
                .objectParts(GetObjectAttributesParts.builder().totalPartsCount(objectPartCount).build())
                .build()
        );
        when(s3AsyncClient.getObjectAttributes(any(GetObjectAttributesRequest.class))).thenReturn(
            getObjectAttributesResponseCompletableFuture
        );

        mockObjectPartResponse(s3AsyncClient, bucketName, blobName, objectPartCount, partSize, objectSize);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountingCompletionListener<ReadContext> readContextActionListener = new CountingCompletionListener<>();
        LatchedActionListener<ReadContext> listener = new LatchedActionListener<>(readContextActionListener, countDownLatch);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
        blobContainer.readBlobAsync(blobName, listener);
        countDownLatch.await();

        assertEquals(1, readContextActionListener.getResponseCount());
        assertEquals(0, readContextActionListener.getFailureCount());
        ReadContext readContext = readContextActionListener.getResponse();
        assertEquals(objectPartCount, readContext.getNumberOfParts());
        assertEquals(checksum, readContext.getBlobChecksum());
        assertEquals(objectSize, readContext.getBlobSize());

        for (int partNumber = 1; partNumber < objectPartCount; partNumber++) {
            InputStreamContainer inputStreamContainer = readContext.getPartStreams().get(partNumber).get().join();
            final int offset = partNumber * partSize;
            assertEquals(partSize, inputStreamContainer.getContentLength());
            assertEquals(offset, inputStreamContainer.getOffset());
            assertEquals(partSize, inputStreamContainer.getInputStream().readAllBytes().length);
        }
    }

    public void testReadBlobAsyncSinglePart() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final String checksum = randomAlphaOfLength(10);

        final int objectSize = 100;

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference amazonAsyncS3Reference = new AmazonAsyncS3Reference(
            AmazonAsyncS3WithCredentials.create(s3AsyncClient, s3AsyncClient, s3AsyncClient, null)
        );
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final BlobPath blobPath = new BlobPath();

        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.serverSideEncryption()).thenReturn(false);
        when(blobStore.asyncClientReference()).thenReturn(amazonAsyncS3Reference);

        CompletableFuture<GetObjectAttributesResponse> getObjectAttributesResponseCompletableFuture = new CompletableFuture<>();
        getObjectAttributesResponseCompletableFuture.complete(
            GetObjectAttributesResponse.builder()
                .checksum(Checksum.builder().checksumCRC32(checksum).build())
                .objectSize((long) objectSize)
                .build()
        );
        when(s3AsyncClient.getObjectAttributes(any(GetObjectAttributesRequest.class))).thenReturn(
            getObjectAttributesResponseCompletableFuture
        );

        mockObjectResponse(s3AsyncClient, bucketName, blobName, objectSize);

        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountingCompletionListener<ReadContext> readContextActionListener = new CountingCompletionListener<>();
        LatchedActionListener<ReadContext> listener = new LatchedActionListener<>(readContextActionListener, countDownLatch);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
        blobContainer.readBlobAsync(blobName, listener);
        countDownLatch.await();

        assertEquals(1, readContextActionListener.getResponseCount());
        assertEquals(0, readContextActionListener.getFailureCount());
        ReadContext readContext = readContextActionListener.getResponse();
        assertEquals(1, readContext.getNumberOfParts());
        assertEquals(checksum, readContext.getBlobChecksum());
        assertEquals(objectSize, readContext.getBlobSize());

        InputStreamContainer inputStreamContainer = readContext.getPartStreams().stream().findFirst().get().get().join();
        assertEquals(objectSize, inputStreamContainer.getContentLength());
        assertEquals(0, inputStreamContainer.getOffset());
        assertEquals(objectSize, inputStreamContainer.getInputStream().readAllBytes().length);

    }

    public void testReadBlobAsyncFailure() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final String checksum = randomAlphaOfLength(10);

        final long objectSize = 100L;
        final int objectPartCount = 10;

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference amazonAsyncS3Reference = new AmazonAsyncS3Reference(
            AmazonAsyncS3WithCredentials.create(s3AsyncClient, s3AsyncClient, s3AsyncClient, null)
        );

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final BlobPath blobPath = new BlobPath();

        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.serverSideEncryption()).thenReturn(false);
        when(blobStore.asyncClientReference()).thenReturn(amazonAsyncS3Reference);

        CompletableFuture<GetObjectAttributesResponse> getObjectAttributesResponseCompletableFuture = new CompletableFuture<>();
        getObjectAttributesResponseCompletableFuture.complete(
            GetObjectAttributesResponse.builder()
                .checksum(Checksum.builder().checksumCRC32(checksum).build())
                .objectSize(objectSize)
                .objectParts(GetObjectAttributesParts.builder().totalPartsCount(objectPartCount).build())
                .build()
        );
        when(s3AsyncClient.getObjectAttributes(any(GetObjectAttributesRequest.class))).thenThrow(new RuntimeException());

        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountingCompletionListener<ReadContext> readContextActionListener = new CountingCompletionListener<>();
        LatchedActionListener<ReadContext> listener = new LatchedActionListener<>(readContextActionListener, countDownLatch);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
        blobContainer.readBlobAsync(blobName, listener);
        countDownLatch.await();

        assertEquals(0, readContextActionListener.getResponseCount());
        assertEquals(1, readContextActionListener.getFailureCount());
    }

    public void testReadBlobAsyncOnCompleteFailureMissingData() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final String checksum = randomAlphaOfLength(10);

        final long objectSize = 100L;
        final int objectPartCount = 10;

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference amazonAsyncS3Reference = new AmazonAsyncS3Reference(
            AmazonAsyncS3WithCredentials.create(s3AsyncClient, s3AsyncClient, s3AsyncClient, null)
        );

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final BlobPath blobPath = new BlobPath();

        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.serverSideEncryption()).thenReturn(false);
        when(blobStore.asyncClientReference()).thenReturn(amazonAsyncS3Reference);

        CompletableFuture<GetObjectAttributesResponse> getObjectAttributesResponseCompletableFuture = new CompletableFuture<>();
        getObjectAttributesResponseCompletableFuture.complete(
            GetObjectAttributesResponse.builder()
                .checksum(Checksum.builder().build())
                .objectSize(null)
                .objectParts(GetObjectAttributesParts.builder().totalPartsCount(objectPartCount).build())
                .build()
        );
        when(s3AsyncClient.getObjectAttributes(any(GetObjectAttributesRequest.class))).thenReturn(
            getObjectAttributesResponseCompletableFuture
        );

        CountDownLatch countDownLatch = new CountDownLatch(1);
        CountingCompletionListener<ReadContext> readContextActionListener = new CountingCompletionListener<>();
        LatchedActionListener<ReadContext> listener = new LatchedActionListener<>(readContextActionListener, countDownLatch);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);
        blobContainer.readBlobAsync(blobName, listener);
        countDownLatch.await();

        assertEquals(0, readContextActionListener.getResponseCount());
        assertEquals(1, readContextActionListener.getFailureCount());
    }

    public void testGetBlobMetadata() throws Exception {
        final String checksum = randomAlphaOfLengthBetween(1, 10);
        final long objectSize = 100L;
        final int objectPartCount = 10;
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final String bucketName = randomAlphaOfLengthBetween(1, 10);

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final BlobPath blobPath = new BlobPath();
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.serverSideEncryption()).thenReturn(false);
        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        CompletableFuture<GetObjectAttributesResponse> getObjectAttributesResponseCompletableFuture = new CompletableFuture<>();
        getObjectAttributesResponseCompletableFuture.complete(
            GetObjectAttributesResponse.builder()
                .checksum(Checksum.builder().checksumCRC32(checksum).build())
                .objectSize(objectSize)
                .objectParts(GetObjectAttributesParts.builder().totalPartsCount(objectPartCount).build())
                .build()
        );
        when(s3AsyncClient.getObjectAttributes(any(GetObjectAttributesRequest.class))).thenReturn(
            getObjectAttributesResponseCompletableFuture
        );

        CompletableFuture<GetObjectAttributesResponse> responseFuture = blobContainer.getBlobMetadata(s3AsyncClient, bucketName, blobName);
        GetObjectAttributesResponse objectAttributesResponse = responseFuture.get();

        assertEquals(checksum, objectAttributesResponse.checksum().checksumCRC32());
        assertEquals(Long.valueOf(objectSize), objectAttributesResponse.objectSize());
        assertEquals(Integer.valueOf(objectPartCount), objectAttributesResponse.objectParts().totalPartsCount());
    }

    public void testGetBlobPartInputStream() throws Exception {
        final String blobName = randomAlphaOfLengthBetween(1, 10);
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final long contentLength = 10L;
        final String contentRange = "bytes 10-20/100";
        final InputStream inputStream = ResponseInputStream.nullInputStream();

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final S3BlobStore blobStore = mock(S3BlobStore.class);
        final BlobPath blobPath = new BlobPath();
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.serverSideEncryption()).thenReturn(false);
        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        GetObjectResponse getObjectResponse = GetObjectResponse.builder().contentLength(contentLength).contentRange(contentRange).build();

        CompletableFuture<ResponseInputStream<GetObjectResponse>> getObjectPartResponse = new CompletableFuture<>();
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(getObjectResponse, inputStream);
        getObjectPartResponse.complete(responseInputStream);

        when(
            s3AsyncClient.getObject(
                any(GetObjectRequest.class),
                ArgumentMatchers.<AsyncResponseTransformer<GetObjectResponse, ResponseInputStream<GetObjectResponse>>>any()
            )
        ).thenReturn(getObjectPartResponse);

        // Header based offset in case of a multi part object request
        InputStreamContainer inputStreamContainer = blobContainer.getBlobPartInputStreamContainer(s3AsyncClient, bucketName, blobName, 0)
            .get();

        assertEquals(10, inputStreamContainer.getOffset());
        assertEquals(contentLength, inputStreamContainer.getContentLength());
        assertEquals(inputStream.available(), inputStreamContainer.getInputStream().available());

        // 0 offset in case of a single part object request
        inputStreamContainer = blobContainer.getBlobPartInputStreamContainer(s3AsyncClient, bucketName, blobName, null).get();

        assertEquals(0, inputStreamContainer.getOffset());
        assertEquals(contentLength, inputStreamContainer.getContentLength());
        assertEquals(inputStream.available(), inputStreamContainer.getInputStream().available());
    }

    public void testTransformResponseToInputStreamContainer() throws Exception {
        final String contentRange = "bytes 0-10/100";
        final long contentLength = 10L;
        final InputStream inputStream = ResponseInputStream.nullInputStream();

        GetObjectResponse getObjectResponse = GetObjectResponse.builder().contentLength(contentLength).build();

        // Exception when content range absent for multipart object
        ResponseInputStream<GetObjectResponse> responseInputStreamNoRange = new ResponseInputStream<>(getObjectResponse, inputStream);
        assertThrows(SdkException.class, () -> S3BlobContainer.transformResponseToInputStreamContainer(responseInputStreamNoRange, true));

        // No exception when content range absent for single part object
        ResponseInputStream<GetObjectResponse> responseInputStreamNoRangeSinglePart = new ResponseInputStream<>(
            getObjectResponse,
            inputStream
        );
        InputStreamContainer inputStreamContainer = S3BlobContainer.transformResponseToInputStreamContainer(
            responseInputStreamNoRangeSinglePart,
            false
        );
        assertEquals(contentLength, inputStreamContainer.getContentLength());
        assertEquals(0, inputStreamContainer.getOffset());

        // Exception when length is absent
        getObjectResponse = GetObjectResponse.builder().contentRange(contentRange).build();
        ResponseInputStream<GetObjectResponse> responseInputStreamNoContentLength = new ResponseInputStream<>(
            getObjectResponse,
            inputStream
        );
        assertThrows(
            SdkException.class,
            () -> S3BlobContainer.transformResponseToInputStreamContainer(responseInputStreamNoContentLength, true)
        );

        // No exception when range and length both are present
        getObjectResponse = GetObjectResponse.builder().contentRange(contentRange).contentLength(contentLength).build();
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(getObjectResponse, inputStream);
        inputStreamContainer = S3BlobContainer.transformResponseToInputStreamContainer(responseInputStream, true);
        assertEquals(contentLength, inputStreamContainer.getContentLength());
        assertEquals(0, inputStreamContainer.getOffset());
        assertEquals(inputStream.available(), inputStreamContainer.getInputStream().available());
    }

    public void testDeleteAsync() throws Exception {
        for (int i = 0; i < 100; i++) {
            testDeleteAsync(i + 1);
        }
    }

    private void testDeleteAsync(int bulkDeleteSize) throws InterruptedException {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.getBulkDeletesSize()).thenReturn(bulkDeleteSize);

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference asyncClientReference = mock(AmazonAsyncS3Reference.class);
        when(blobStore.asyncClientReference()).thenReturn(asyncClientReference);
        AmazonAsyncS3WithCredentials amazonAsyncS3WithCredentials = AmazonAsyncS3WithCredentials.create(
            s3AsyncClient,
            s3AsyncClient,
            s3AsyncClient,
            null
        );
        when(asyncClientReference.get()).thenReturn(amazonAsyncS3WithCredentials);

        final List<S3Object> s3Objects = new ArrayList<>();
        int numObjects = randomIntBetween(20, 100);
        long totalSize = 0;
        for (int i = 0; i < numObjects; i++) {
            long size = randomIntBetween(1, 100);
            s3Objects.add(S3Object.builder().key(randomAlphaOfLength(10)).size(size).build());
            totalSize += size;
        }

        final List<ListObjectsV2Response> responseList = new ArrayList<>();
        int size = 0;
        while (size < numObjects) {
            int toAdd = randomIntBetween(10, 20);
            int endIndex = Math.min(numObjects, size + toAdd);
            responseList.add(ListObjectsV2Response.builder().contents(s3Objects.subList(size, endIndex)).build());
            size = endIndex;
        }
        int expectedDeletedObjectsCall = numObjects / bulkDeleteSize + (numObjects % bulkDeleteSize > 0 ? 1 : 0);

        final ListObjectsV2Publisher listPublisher = mock(ListObjectsV2Publisher.class);
        AtomicInteger counter = new AtomicInteger();
        doAnswer(invocation -> {
            Subscriber<? super ListObjectsV2Response> subscriber = invocation.getArgument(0);
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    int currentCounter = counter.getAndIncrement();
                    if (currentCounter < responseList.size()) {
                        subscriber.onNext(responseList.get(currentCounter));
                    }
                    if (currentCounter == responseList.size()) {
                        subscriber.onComplete();
                    }
                }

                @Override
                public void cancel() {}
            });
            return null;
        }).when(listPublisher).subscribe(ArgumentMatchers.<Subscriber<ListObjectsV2Response>>any());
        when(s3AsyncClient.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(listPublisher);

        when(s3AsyncClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(
            CompletableFuture.completedFuture(DeleteObjectsResponse.builder().build())
        );

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<DeleteResult> deleteResultRef = new AtomicReference<>();
        blobContainer.deleteAsync(new ActionListener<>() {
            @Override
            public void onResponse(DeleteResult deleteResult) {
                deleteResultRef.set(deleteResult);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("exception during deleteAsync", e);
                fail("Unexpected failure: " + e.getMessage());
            }
        });

        latch.await();

        DeleteResult deleteResult = deleteResultRef.get();
        assertEquals(numObjects, deleteResult.blobsDeleted());
        assertEquals(totalSize, deleteResult.bytesDeleted());

        verify(s3AsyncClient, times(1)).listObjectsV2Paginator(any(ListObjectsV2Request.class));
        verify(s3AsyncClient, times(expectedDeletedObjectsCall)).deleteObjects(any(DeleteObjectsRequest.class));
    }

    public void testDeleteAsyncFailure() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.getBulkDeletesSize()).thenReturn(1000);

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference asyncClientReference = mock(AmazonAsyncS3Reference.class);
        when(blobStore.asyncClientReference()).thenReturn(asyncClientReference);
        AmazonAsyncS3WithCredentials amazonAsyncS3WithCredentials = AmazonAsyncS3WithCredentials.create(
            s3AsyncClient,
            s3AsyncClient,
            s3AsyncClient,
            null
        );
        when(asyncClientReference.get()).thenReturn(amazonAsyncS3WithCredentials);

        // Simulate a failure in listObjectsV2Paginator
        RuntimeException simulatedFailure = new RuntimeException("Simulated failure");
        when(s3AsyncClient.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenThrow(simulatedFailure);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        blobContainer.deleteAsync(new ActionListener<>() {
            @Override
            public void onResponse(DeleteResult deleteResult) {
                fail("Expected a failure, but got a success response");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        latch.await();

        assertNotNull(exceptionRef.get());
        assertEquals(IOException.class, exceptionRef.get().getClass());
        assertEquals("Failed to initiate async deletion", exceptionRef.get().getMessage());
        assertEquals(simulatedFailure, exceptionRef.get().getCause());
    }

    public void testDeleteAsyncListingError() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.getBulkDeletesSize()).thenReturn(1000);

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference asyncClientReference = mock(AmazonAsyncS3Reference.class);
        when(blobStore.asyncClientReference()).thenReturn(asyncClientReference);
        AmazonAsyncS3WithCredentials amazonAsyncS3WithCredentials = AmazonAsyncS3WithCredentials.create(
            s3AsyncClient,
            s3AsyncClient,
            s3AsyncClient,
            null
        );
        when(asyncClientReference.get()).thenReturn(amazonAsyncS3WithCredentials);

        final ListObjectsV2Publisher listPublisher = mock(ListObjectsV2Publisher.class);
        doAnswer(invocation -> {
            Subscriber<? super ListObjectsV2Response> subscriber = invocation.getArgument(0);
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    subscriber.onError(new RuntimeException("Simulated listing error"));
                }

                @Override
                public void cancel() {}
            });
            return null;
        }).when(listPublisher).subscribe(ArgumentMatchers.<Subscriber<ListObjectsV2Response>>any());
        when(s3AsyncClient.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(listPublisher);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        blobContainer.deleteAsync(new ActionListener<>() {
            @Override
            public void onResponse(DeleteResult deleteResult) {
                fail("Expected a failure, but got a success response");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        latch.await();

        assertNotNull(exceptionRef.get());
        assertEquals(IOException.class, exceptionRef.get().getClass());
        assertEquals("Failed to list objects for deletion", exceptionRef.get().getMessage());
        assertNotNull(exceptionRef.get().getCause());
        assertEquals("Simulated listing error", exceptionRef.get().getCause().getMessage());
    }

    public void testDeleteAsyncDeletionError() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.getBulkDeletesSize()).thenReturn(1000);

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference asyncClientReference = mock(AmazonAsyncS3Reference.class);
        when(blobStore.asyncClientReference()).thenReturn(asyncClientReference);
        AmazonAsyncS3WithCredentials amazonAsyncS3WithCredentials = AmazonAsyncS3WithCredentials.create(
            s3AsyncClient,
            s3AsyncClient,
            s3AsyncClient,
            null
        );
        when(asyncClientReference.get()).thenReturn(amazonAsyncS3WithCredentials);

        final ListObjectsV2Publisher listPublisher = mock(ListObjectsV2Publisher.class);
        doAnswer(invocation -> {
            Subscriber<? super ListObjectsV2Response> subscriber = invocation.getArgument(0);
            subscriber.onSubscribe(new Subscription() {
                @Override
                public void request(long n) {
                    subscriber.onNext(
                        ListObjectsV2Response.builder().contents(S3Object.builder().key("test-key").size(100L).build()).build()
                    );
                    subscriber.onComplete();
                }

                @Override
                public void cancel() {}
            });
            return null;
        }).when(listPublisher).subscribe(ArgumentMatchers.<Subscriber<ListObjectsV2Response>>any());
        when(s3AsyncClient.listObjectsV2Paginator(any(ListObjectsV2Request.class))).thenReturn(listPublisher);

        // Simulate a failure in deleteObjects
        CompletableFuture<DeleteObjectsResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Simulated delete error"));
        when(s3AsyncClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(failedFuture);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        blobContainer.deleteAsync(new ActionListener<>() {
            @Override
            public void onResponse(DeleteResult deleteResult) {
                fail("Expected a failure, but got a success response");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        latch.await();

        assertNotNull(exceptionRef.get());
        assertEquals(CompletionException.class, exceptionRef.get().getClass());
        assertEquals("java.lang.RuntimeException: Simulated delete error", exceptionRef.get().getMessage());
        assertNotNull(exceptionRef.get().getCause());
        assertEquals("Simulated delete error", exceptionRef.get().getCause().getMessage());
    }

    public void testDeleteBlobsAsyncIgnoringIfNotExists() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        int bulkDeleteSize = randomIntBetween(1, 10);
        when(blobStore.getBulkDeletesSize()).thenReturn(bulkDeleteSize);

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference asyncClientReference = mock(AmazonAsyncS3Reference.class);
        when(blobStore.asyncClientReference()).thenReturn(asyncClientReference);
        AmazonAsyncS3WithCredentials amazonAsyncS3WithCredentials = AmazonAsyncS3WithCredentials.create(
            s3AsyncClient,
            s3AsyncClient,
            s3AsyncClient,
            null
        );
        when(asyncClientReference.get()).thenReturn(amazonAsyncS3WithCredentials);

        final List<String> blobNames = new ArrayList<>();
        int size = randomIntBetween(10, 100);
        for (int i = 0; i < size; i++) {
            blobNames.add(randomAlphaOfLength(10));
        }
        int expectedDeleteCalls = size / bulkDeleteSize + (size % bulkDeleteSize > 0 ? 1 : 0);

        when(s3AsyncClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(
            CompletableFuture.completedFuture(DeleteObjectsResponse.builder().build())
        );

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        blobContainer.deleteBlobsAsyncIgnoringIfNotExists(blobNames, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        latch.await();

        assertNull(exceptionRef.get());

        ArgumentCaptor<DeleteObjectsRequest> deleteRequestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(s3AsyncClient, times(expectedDeleteCalls)).deleteObjects(deleteRequestCaptor.capture());

        DeleteObjectsRequest capturedRequest = deleteRequestCaptor.getAllValues().stream().findAny().get();
        assertEquals(bucketName, capturedRequest.bucket());
        int totalBlobsDeleted = deleteRequestCaptor.getAllValues()
            .stream()
            .map(r -> r.delete().objects().size())
            .reduce(Integer::sum)
            .get();
        assertEquals(blobNames.size(), totalBlobsDeleted);
        List<String> deletedKeys = deleteRequestCaptor.getAllValues()
            .stream()
            .flatMap(r -> r.delete().objects().stream())
            .map(ObjectIdentifier::key)
            .collect(Collectors.toList());
        assertTrue(deletedKeys.containsAll(blobNames));
    }

    public void testDeleteBlobsAsyncIgnoringIfNotExistsFailure() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.getBulkDeletesSize()).thenReturn(1000);

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference asyncClientReference = mock(AmazonAsyncS3Reference.class);
        when(blobStore.asyncClientReference()).thenReturn(asyncClientReference);
        AmazonAsyncS3WithCredentials amazonAsyncS3WithCredentials = AmazonAsyncS3WithCredentials.create(
            s3AsyncClient,
            s3AsyncClient,
            s3AsyncClient,
            null
        );
        when(asyncClientReference.get()).thenReturn(amazonAsyncS3WithCredentials);

        // Simulate a failure in deleteObjects
        CompletableFuture<DeleteObjectsResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Simulated delete failure"));
        when(s3AsyncClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(failedFuture);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        List<String> blobNames = Arrays.asList("blob1", "blob2", "blob3");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();
        blobContainer.deleteBlobsAsyncIgnoringIfNotExists(blobNames, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                fail("Expected a failure, but got a success response");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        latch.await();

        assertNotNull(exceptionRef.get());
        assertEquals(IOException.class, exceptionRef.get().getClass());
        assertEquals("Failed to delete blobs " + blobNames, exceptionRef.get().getMessage());
        assertNotNull(exceptionRef.get().getCause());
        assertEquals("java.lang.RuntimeException: Simulated delete failure", exceptionRef.get().getCause().getMessage());
    }

    public void testDeleteBlobsAsyncIgnoringIfNotExistsWithEmptyList() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.getBulkDeletesSize()).thenReturn(1000);

        final S3AsyncClient s3AsyncClient = mock(S3AsyncClient.class);
        final AmazonAsyncS3Reference asyncClientReference = mock(AmazonAsyncS3Reference.class);
        when(blobStore.asyncClientReference()).thenReturn(asyncClientReference);
        AmazonAsyncS3WithCredentials amazonAsyncS3WithCredentials = AmazonAsyncS3WithCredentials.create(
            s3AsyncClient,
            s3AsyncClient,
            s3AsyncClient,
            null
        );
        when(asyncClientReference.get()).thenReturn(amazonAsyncS3WithCredentials);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        List<String> emptyBlobNames = Collections.emptyList();

        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean onResponseCalled = new AtomicBoolean(false);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        blobContainer.deleteBlobsAsyncIgnoringIfNotExists(emptyBlobNames, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                onResponseCalled.set(true);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        latch.await();

        assertTrue("onResponse should have been called", onResponseCalled.get());
        assertNull("No exception should have been thrown", exceptionRef.get());

        // Verify that no interactions with S3AsyncClient occurred
        verifyNoInteractions(s3AsyncClient);
    }

    public void testDeleteBlobsAsyncIgnoringIfNotExistsInitializationFailure() throws Exception {
        final String bucketName = randomAlphaOfLengthBetween(1, 10);
        final BlobPath blobPath = new BlobPath();

        final S3BlobStore blobStore = mock(S3BlobStore.class);
        when(blobStore.bucket()).thenReturn(bucketName);
        when(blobStore.getStatsMetricPublisher()).thenReturn(new StatsMetricPublisher());
        when(blobStore.getBulkDeletesSize()).thenReturn(1000);

        // Simulate a failure when getting the asyncClientReference
        RuntimeException simulatedFailure = new RuntimeException("Simulated initialization failure");
        when(blobStore.asyncClientReference()).thenThrow(simulatedFailure);

        final S3BlobContainer blobContainer = new S3BlobContainer(blobPath, blobStore);

        List<String> blobNames = Arrays.asList("blob1", "blob2", "blob3");

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        blobContainer.deleteBlobsAsyncIgnoringIfNotExists(blobNames, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                fail("Expected a failure, but got a success response");
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        latch.await();

        assertNotNull("An exception should have been thrown", exceptionRef.get());
        assertTrue("Exception should be an IOException", exceptionRef.get() instanceof IOException);
        assertEquals("Failed to initiate async blob deletion", exceptionRef.get().getMessage());
        assertEquals(simulatedFailure, exceptionRef.get().getCause());
    }

    private void mockObjectResponse(S3AsyncClient s3AsyncClient, String bucketName, String blobName, int objectSize) {

        final InputStream inputStream = new ByteArrayInputStream(randomByteArrayOfLength(objectSize));

        GetObjectResponse getObjectResponse = GetObjectResponse.builder().contentLength((long) objectSize).build();

        CompletableFuture<ResponseInputStream<GetObjectResponse>> getObjectPartResponse = new CompletableFuture<>();
        ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(getObjectResponse, inputStream);
        getObjectPartResponse.complete(responseInputStream);

        GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(blobName).build();

        when(
            s3AsyncClient.getObject(
                eq(getObjectRequest),
                ArgumentMatchers.<AsyncResponseTransformer<GetObjectResponse, ResponseInputStream<GetObjectResponse>>>any()
            )
        ).thenReturn(getObjectPartResponse);

    }

    private void mockObjectPartResponse(
        S3AsyncClient s3AsyncClient,
        String bucketName,
        String blobName,
        int totalNumberOfParts,
        int partSize,
        long objectSize
    ) {
        for (int partNumber = 1; partNumber <= totalNumberOfParts; partNumber++) {
            final int start = (partNumber - 1) * partSize;
            final int end = partNumber * partSize;
            final String contentRange = "bytes " + start + "-" + end + "/" + objectSize;
            final InputStream inputStream = new ByteArrayInputStream(randomByteArrayOfLength(partSize));

            GetObjectResponse getObjectResponse = GetObjectResponse.builder()
                .contentLength((long) partSize)
                .contentRange(contentRange)
                .build();

            CompletableFuture<ResponseInputStream<GetObjectResponse>> getObjectPartResponse = new CompletableFuture<>();
            ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(getObjectResponse, inputStream);
            getObjectPartResponse.complete(responseInputStream);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder().bucket(bucketName).key(blobName).partNumber(partNumber).build();

            when(
                s3AsyncClient.getObject(
                    eq(getObjectRequest),
                    ArgumentMatchers.<AsyncResponseTransformer<GetObjectResponse, ResponseInputStream<GetObjectResponse>>>any()
                )
            ).thenReturn(getObjectPartResponse);
        }
    }

    private static class CountingCompletionListener<T> implements ActionListener<T> {
        private int responseCount;
        private int failureCount;
        private T response;
        private Exception exception;

        @Override
        public void onResponse(T response) {
            this.response = response;
            responseCount++;
        }

        @Override
        public void onFailure(Exception e) {
            exception = e;
            failureCount++;
        }

        public int getResponseCount() {
            return responseCount;
        }

        public int getFailureCount() {
            return failureCount;
        }

        public T getResponse() {
            return response;
        }

        public Exception getException() {
            return exception;
        }
    }
}
