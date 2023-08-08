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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.StreamContext;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.VerifyingMultiStreamBlobContainer;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.AbstractBlobContainer;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import org.opensearch.core.common.Strings;
import org.opensearch.repositories.s3.async.UploadRequest;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opensearch.repositories.s3.S3Repository.MAX_FILE_SIZE;
import static org.opensearch.repositories.s3.S3Repository.MAX_FILE_SIZE_USING_MULTIPART;
import static org.opensearch.repositories.s3.S3Repository.MIN_PART_SIZE_USING_MULTIPART;

class S3BlobContainer extends AbstractBlobContainer implements VerifyingMultiStreamBlobContainer {

    private static final Logger logger = LogManager.getLogger(S3BlobContainer.class);

    /**
     * Maximum number of deletes in a {@link DeleteObjectsRequest}.
     *
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html">S3 Documentation</a>.
     */
    private static final int MAX_BULK_DELETES = 1000;

    private final S3BlobStore blobStore;
    private final String keyPath;

    S3BlobContainer(BlobPath path, S3BlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
    }

    @Override
    public boolean blobExists(String blobName) {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            SocketAccess.doPrivileged(
                () -> clientReference.get()
                    .headObject(HeadObjectRequest.builder().bucket(blobStore.bucket()).key(buildKey(blobName)).build())
            );
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (final Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return new S3RetryingInputStream(blobStore, buildKey(blobName));
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else {
            return new S3RetryingInputStream(blobStore, buildKey(blobName), position, Math.addExact(position, length - 1));
        }
    }

    @Override
    public long readBlobPreferredLength() {
        // This container returns streams that must be fully consumed, so we tell consumers to make bounded requests.
        return new ByteSizeValue(32, ByteSizeUnit.MB).getBytes();
    }

    /**
     * This implementation ignores the failIfAlreadyExists flag as the S3 API has no way to enforce this due to its weak consistency model.
     */
    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        assert inputStream.markSupported() : "No mark support on inputStream breaks the S3 SDK's ability to retry requests";
        SocketAccess.doPrivilegedIOException(() -> {
            if (blobSize <= getLargeBlobThresholdInBytes()) {
                executeSingleUpload(blobStore, buildKey(blobName), inputStream, blobSize);
            } else {
                executeMultipartUpload(blobStore, buildKey(blobName), inputStream, blobSize);
            }
            return null;
        });
    }

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        UploadRequest uploadRequest = new UploadRequest(
            blobStore.bucket(),
            buildKey(writeContext.getFileName()),
            writeContext.getFileSize(),
            writeContext.getWritePriority(),
            writeContext.getUploadFinalizer(),
            writeContext.doRemoteDataIntegrityCheck(),
            writeContext.getExpectedChecksum()
        );
        try {
            long partSize = blobStore.getAsyncTransferManager().calculateOptimalPartSize(writeContext.getFileSize());
            StreamContext streamContext = SocketAccess.doPrivileged(() -> writeContext.getStreamProvider(partSize));
            try (AmazonAsyncS3Reference amazonS3Reference = SocketAccess.doPrivileged(blobStore::asyncClientReference)) {

                S3AsyncClient s3AsyncClient = writeContext.getWritePriority() == WritePriority.HIGH
                    ? amazonS3Reference.get().priorityClient()
                    : amazonS3Reference.get().client();
                CompletableFuture<Void> completableFuture = blobStore.getAsyncTransferManager()
                    .uploadObject(s3AsyncClient, uploadRequest, streamContext);
                completableFuture.whenComplete((response, throwable) -> {
                    if (throwable == null) {
                        completionListener.onResponse(response);
                    } else {
                        Exception ex = throwable instanceof Error ? new Exception(throwable) : (Exception) throwable;
                        completionListener.onFailure(ex);
                    }
                });
            }
        } catch (Exception e) {
            logger.info("exception error from blob container for file {}", writeContext.getFileName());
            throw new IOException(e);
        }
    }

    // package private for testing
    long getLargeBlobThresholdInBytes() {
        return blobStore.bufferSizeInBytes();
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete() throws IOException {
        final AtomicLong deletedBlobs = new AtomicLong();
        final AtomicLong deletedBytes = new AtomicLong();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            ListObjectsV2Iterable listObjectsIterable = SocketAccess.doPrivileged(
                () -> clientReference.get()
                    .listObjectsV2Paginator(
                        ListObjectsV2Request.builder()
                            .bucket(blobStore.bucket())
                            .prefix(keyPath)
                            .overrideConfiguration(
                                o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().listObjectsMetricPublisher)
                            )
                            .build()
                    )
            );

            Iterator<ListObjectsV2Response> listObjectsResponseIterator = listObjectsIterable.iterator();
            while (listObjectsResponseIterator.hasNext()) {
                ListObjectsV2Response listObjectsResponse = SocketAccess.doPrivileged(listObjectsResponseIterator::next);
                List<String> blobsToDelete = listObjectsResponse.contents().stream().map(s3Object -> {
                    deletedBlobs.incrementAndGet();
                    deletedBytes.addAndGet(s3Object.size());

                    return s3Object.key();
                }).collect(Collectors.toList());

                if (!listObjectsResponseIterator.hasNext()) {
                    blobsToDelete.add(keyPath);
                }

                doDeleteBlobs(blobsToDelete, false);
            }
        } catch (SdkException e) {
            throw new IOException("Exception when deleting blob container [" + keyPath + "]", e);
        }

        return new DeleteResult(deletedBlobs.get(), deletedBytes.get());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        doDeleteBlobs(blobNames, true);
    }

    private void doDeleteBlobs(List<String> blobNames, boolean relative) throws IOException {
        if (blobNames.isEmpty()) {
            return;
        }
        final Set<String> outstanding;
        if (relative) {
            outstanding = blobNames.stream().map(this::buildKey).collect(Collectors.toSet());
        } else {
            outstanding = new HashSet<>(blobNames);
        }
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            // S3 API only allows 1k blobs per delete so we split up the given blobs into requests of max. 1k deletes
            final List<DeleteObjectsRequest> deleteRequests = new ArrayList<>();
            final List<String> partition = new ArrayList<>();
            for (String key : outstanding) {
                partition.add(key);
                if (partition.size() == MAX_BULK_DELETES) {
                    deleteRequests.add(bulkDelete(blobStore.bucket(), partition));
                    partition.clear();
                }
            }
            if (partition.isEmpty() == false) {
                deleteRequests.add(bulkDelete(blobStore.bucket(), partition));
            }
            SocketAccess.doPrivilegedVoid(() -> {
                SdkException aex = null;
                for (DeleteObjectsRequest deleteRequest : deleteRequests) {
                    List<String> keysInRequest = deleteRequest.delete()
                        .objects()
                        .stream()
                        .map(ObjectIdentifier::key)
                        .collect(Collectors.toList());
                    try {
                        DeleteObjectsResponse deleteObjectsResponse = clientReference.get().deleteObjects(deleteRequest);
                        outstanding.removeAll(keysInRequest);
                        outstanding.addAll(deleteObjectsResponse.errors().stream().map(S3Error::key).collect(Collectors.toSet()));
                        if (!deleteObjectsResponse.errors().isEmpty()) {
                            logger.warn(
                                () -> new ParameterizedMessage(
                                    "Failed to delete some blobs {}",
                                    deleteObjectsResponse.errors()
                                        .stream()
                                        .map(s3Error -> "[" + s3Error.key() + "][" + s3Error.code() + "][" + s3Error.message() + "]")
                                        .collect(Collectors.toList())
                                )
                            );
                        }
                    } catch (SdkException e) {
                        // The AWS client threw any unexpected exception and did not execute the request at all so we do not
                        // remove any keys from the outstanding deletes set.
                        aex = ExceptionsHelper.useOrSuppress(aex, e);
                    }
                }
                if (aex != null) {
                    throw aex;
                }
            });
        } catch (Exception e) {
            throw new IOException("Failed to delete blobs [" + outstanding + "]", e);
        }
        assert outstanding.isEmpty();
    }

    private static DeleteObjectsRequest bulkDelete(String bucket, List<String> blobs) {
        return DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(
                Delete.builder()
                    .objects(blobs.stream().map(blob -> ObjectIdentifier.builder().key(blob).build()).collect(Collectors.toList()))
                    .quiet(true)
                    .build()
            )
            .build();
    }

    @Override
    public void listBlobsByPrefixInSortedOrder(
        String blobNamePrefix,
        int limit,
        BlobNameSortOrder blobNameSortOrder,
        ActionListener<List<BlobMetadata>> listener
    ) {
        // As AWS S3 returns list of keys in Lexicographic order, we don't have to fetch all the keys in order to sort them
        // We fetch only keys as per the given limit to optimize the fetch. If provided sort order is not Lexicographic,
        // we fall-back to default implementation of fetching all the keys and sorting them.
        if (blobNameSortOrder != BlobNameSortOrder.LEXICOGRAPHIC) {
            super.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, blobNameSortOrder, listener);
        } else {
            if (limit < 0) {
                throw new IllegalArgumentException("limit should not be a negative value");
            }
            String prefix = blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix);
            try (AmazonS3Reference clientReference = blobStore.clientReference()) {
                List<BlobMetadata> blobs = executeListing(clientReference, listObjectsRequest(prefix, limit), limit).stream()
                    .flatMap(listing -> listing.contents().stream())
                    .map(s3Object -> new PlainBlobMetadata(s3Object.key().substring(keyPath.length()), s3Object.size()))
                    .collect(Collectors.toList());
                listener.onResponse(blobs.subList(0, Math.min(limit, blobs.size())));
            } catch (final Exception e) {
                listener.onFailure(new IOException("Exception when listing blobs by prefix [" + prefix + "]", e));
            }
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        String prefix = blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix);
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return executeListing(clientReference, listObjectsRequest(prefix)).stream()
                .flatMap(listing -> listing.contents().stream())
                .map(s3Object -> new PlainBlobMetadata(s3Object.key().substring(keyPath.length()), s3Object.size()))
                .collect(Collectors.toMap(PlainBlobMetadata::name, Function.identity()));
        } catch (final SdkException e) {
            throw new IOException("Exception when listing blobs by prefix [" + prefix + "]", e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return executeListing(clientReference, listObjectsRequest(keyPath)).stream().flatMap(listObjectsResponse -> {
                assert listObjectsResponse.contents().stream().noneMatch(s -> {
                    for (CommonPrefix commonPrefix : listObjectsResponse.commonPrefixes()) {
                        if (s.key().substring(keyPath.length()).startsWith(commonPrefix.prefix())) {
                            return true;
                        }
                    }
                    return false;
                }) : "Response contained children for listed common prefixes.";
                return listObjectsResponse.commonPrefixes().stream();
            })
                .map(commonPrefix -> commonPrefix.prefix().substring(keyPath.length()))
                .filter(name -> name.isEmpty() == false)
                // Stripping the trailing slash off of the common prefix
                .map(name -> name.substring(0, name.length() - 1))
                .collect(Collectors.toMap(Function.identity(), name -> blobStore.blobContainer(path().add(name))));
        } catch (final SdkException e) {
            throw new IOException("Exception when listing children of [" + path().buildAsString() + ']', e);
        }
    }

    private static List<ListObjectsV2Response> executeListing(AmazonS3Reference clientReference, ListObjectsV2Request listObjectsRequest) {
        return executeListing(clientReference, listObjectsRequest, -1);
    }

    private static List<ListObjectsV2Response> executeListing(
        AmazonS3Reference clientReference,
        ListObjectsV2Request listObjectsRequest,
        int limit
    ) {
        return SocketAccess.doPrivileged(() -> {
            final List<ListObjectsV2Response> results = new ArrayList<>();
            int totalObjects = 0;
            ListObjectsV2Iterable listObjectsIterable = clientReference.get().listObjectsV2Paginator(listObjectsRequest);
            for (ListObjectsV2Response listObjectsV2Response : listObjectsIterable) {
                results.add(listObjectsV2Response);
                totalObjects += listObjectsV2Response.contents().size();
                if (limit != -1 && totalObjects > limit) {
                    break;
                }
            }
            return results;
        });
    }

    private ListObjectsV2Request listObjectsRequest(String keyPath) {
        return ListObjectsV2Request.builder()
            .bucket(blobStore.bucket())
            .prefix(keyPath)
            .delimiter("/")
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().listObjectsMetricPublisher))
            .build();
    }

    private ListObjectsV2Request listObjectsRequest(String keyPath, int limit) {
        return listObjectsRequest(keyPath).toBuilder().maxKeys(Math.min(limit, 1000)).build();
    }

    private String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * Uploads a blob using a single upload request
     */
    void executeSingleUpload(final S3BlobStore blobStore, final String blobName, final InputStream input, final long blobSize)
        throws IOException {

        // Extra safety checks
        if (blobSize > MAX_FILE_SIZE.getBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than " + MAX_FILE_SIZE);
        }
        if (blobSize > blobStore.bufferSizeInBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than buffer size");
        }

        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
            .bucket(blobStore.bucket())
            .key(blobName)
            .contentLength(blobSize)
            .storageClass(blobStore.getStorageClass())
            .acl(blobStore.getCannedACL())
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().putObjectMetricPublisher));
        if (blobStore.serverSideEncryption()) {
            putObjectRequestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
        }

        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            SocketAccess.doPrivilegedVoid(
                () -> clientReference.get().putObject(putObjectRequest, RequestBody.fromInputStream(input, blobSize))
            );
        } catch (final SdkException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using a single upload", e);
        }
    }

    /**
     * Uploads a blob using multipart upload requests.
     */
    void executeMultipartUpload(final S3BlobStore blobStore, final String blobName, final InputStream input, final long blobSize)
        throws IOException {

        ensureMultiPartUploadSize(blobSize);
        final long partSize = blobStore.bufferSizeInBytes();
        final Tuple<Long, Long> multiparts = numberOfMultiparts(blobSize, partSize);

        if (multiparts.v1() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too many multipart upload requests, maybe try a larger buffer size?");
        }

        final int nbParts = multiparts.v1().intValue();
        final long lastPartSize = multiparts.v2();
        assert blobSize == (((nbParts - 1) * partSize) + lastPartSize) : "blobSize does not match multipart sizes";

        final SetOnce<String> uploadId = new SetOnce<>();
        final String bucketName = blobStore.bucket();
        boolean success = false;

        CreateMultipartUploadRequest.Builder createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder()
            .bucket(bucketName)
            .key(blobName)
            .storageClass(blobStore.getStorageClass())
            .acl(blobStore.getCannedACL())
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().multipartUploadMetricCollector));

        if (blobStore.serverSideEncryption()) {
            createMultipartUploadRequestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
        }

        CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            uploadId.set(
                SocketAccess.doPrivileged(() -> clientReference.get().createMultipartUpload(createMultipartUploadRequest).uploadId())
            );
            if (Strings.isEmpty(uploadId.get())) {
                throw new IOException("Failed to initialize multipart upload " + blobName);
            }

            final List<CompletedPart> parts = new ArrayList<>();

            long bytesCount = 0;
            for (int i = 1; i <= nbParts; i++) {
                final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
                    .bucket(bucketName)
                    .key(blobName)
                    .uploadId(uploadId.get())
                    .partNumber(i)
                    .contentLength((i < nbParts) ? partSize : lastPartSize)
                    .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().multipartUploadMetricCollector))
                    .build();

                bytesCount += uploadPartRequest.contentLength();

                final UploadPartResponse uploadResponse = SocketAccess.doPrivileged(
                    () -> clientReference.get()
                        .uploadPart(uploadPartRequest, RequestBody.fromInputStream(input, uploadPartRequest.contentLength()))
                );
                parts.add(CompletedPart.builder().partNumber(uploadPartRequest.partNumber()).eTag(uploadResponse.eTag()).build());
            }

            if (bytesCount != blobSize) {
                throw new IOException(
                    "Failed to execute multipart upload for [" + blobName + "], expected " + blobSize + "bytes sent but got " + bytesCount
                );
            }

            CompleteMultipartUploadRequest completeMultipartUploadRequest = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(blobName)
                .uploadId(uploadId.get())
                .multipartUpload(CompletedMultipartUpload.builder().parts(parts).build())
                .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().multipartUploadMetricCollector))
                .build();

            SocketAccess.doPrivilegedVoid(() -> clientReference.get().completeMultipartUpload(completeMultipartUploadRequest));
            success = true;

        } catch (final SdkException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } finally {
            if ((success == false) && Strings.hasLength(uploadId.get())) {
                AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(blobName)
                    .uploadId(uploadId.get())
                    .build();
                try (AmazonS3Reference clientReference = blobStore.clientReference()) {
                    SocketAccess.doPrivilegedVoid(() -> clientReference.get().abortMultipartUpload(abortRequest));
                }
            }
        }
    }

    // non-static, package private for testing
    void ensureMultiPartUploadSize(final long blobSize) {
        if (blobSize > MAX_FILE_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException(
                "Multipart upload request size [" + blobSize + "] can't be larger than " + MAX_FILE_SIZE_USING_MULTIPART
            );
        }
        if (blobSize < MIN_PART_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException(
                "Multipart upload request size [" + blobSize + "] can't be smaller than " + MIN_PART_SIZE_USING_MULTIPART
            );
        }
    }

    /**
     * Returns the number parts of size of {@code partSize} needed to reach {@code totalSize},
     * along with the size of the last (or unique) part.
     *
     * @param totalSize the total size
     * @param partSize  the part size
     * @return a {@link Tuple} containing the number of parts to fill {@code totalSize} and
     * the size of the last part
     */
    static Tuple<Long, Long> numberOfMultiparts(final long totalSize, final long partSize) {
        if (partSize <= 0) {
            throw new IllegalArgumentException("Part size must be greater than zero");
        }

        if ((totalSize == 0L) || (totalSize <= partSize)) {
            return Tuple.tuple(1L, totalSize);
        }

        final long parts = totalSize / partSize;
        final long remaining = totalSize % partSize;

        if (remaining == 0) {
            return Tuple.tuple(parts, partSize);
        } else {
            return Tuple.tuple(parts + 1, remaining);
        }
    }
}
