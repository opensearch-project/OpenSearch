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

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesRequest;
import software.amazon.awssdk.services.s3.model.GetObjectAttributesResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.ObjectAttributes;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Publisher;
import software.amazon.awssdk.utils.CollectionUtils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.cluster.metadata.CryptoMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.SetOnce;
import org.opensearch.common.StreamContext;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.BlobStoreException;
import org.opensearch.common.blobstore.BlobVersionConflictException;
import org.opensearch.common.blobstore.DeleteResult;
import org.opensearch.common.blobstore.InputStreamWithMetadata;
import org.opensearch.common.blobstore.ServerSideCopyBlobContainer;
import org.opensearch.common.blobstore.ServerSideCopySource;
import org.opensearch.common.blobstore.VersionedBlob;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.blobstore.stream.write.WriteContext;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.support.AbstractBlobContainer;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.common.util.concurrent.FutureUtils;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.repositories.s3.async.S3AsyncDeleteHelper;
import org.opensearch.repositories.s3.async.SizeBasedBlockingQ;
import org.opensearch.repositories.s3.async.UploadRequest;
import org.opensearch.repositories.s3.utils.HttpRangeUtils;
import org.opensearch.repositories.s3.utils.SseKmsUtil;
import org.opensearch.secure_sm.AccessController;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.opensearch.repositories.s3.S3Repository.MAX_FILE_SIZE;
import static org.opensearch.repositories.s3.S3Repository.MAX_FILE_SIZE_USING_MULTIPART;
import static org.opensearch.repositories.s3.S3Repository.MIN_PART_SIZE_USING_MULTIPART;
import static org.opensearch.repositories.s3.utils.SseKmsUtil.configureEncryptionSettings;

class S3BlobContainer extends AbstractBlobContainer implements AsyncMultiStreamBlobContainer, ServerSideCopyBlobContainer {

    private static final Logger logger = LogManager.getLogger(S3BlobContainer.class);
    private static final long DEFAULT_OPERATION_TIMEOUT = TimeUnit.SECONDS.toSeconds(30);

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
            AccessController.doPrivileged(
                () -> clientReference.get()
                    .headObject(
                        HeadObjectRequest.builder()
                            .bucket(blobStore.bucket())
                            .key(buildKey(blobName))
                            .expectedBucketOwner(blobStore.expectedBucketOwner())
                            .build()
                    )
            );
            return true;
        } catch (NoSuchKeyException e) {
            return false;
        } catch (final Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @ExperimentalApi
    @Override
    public InputStreamWithMetadata readBlobWithMetadata(String blobName) throws IOException {
        S3RetryingInputStream s3RetryingInputStream = new S3RetryingInputStream(blobStore, buildKey(blobName));
        return new InputStreamWithMetadata(s3RetryingInputStream, s3RetryingInputStream.getMetadata());
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
        writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, null);
    }

    /**
     * Write blob with its object metadata and optional encryption settings.
     */
    @ExperimentalApi
    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        @Nullable Map<String, String> metadata,
        @Nullable CryptoMetadata cryptoMetadata
    ) throws IOException {
        assert inputStream.markSupported() : "No mark support on inputStream breaks the S3 SDK's ability to retry requests";
        AccessController.doPrivilegedChecked(() -> {
            if (blobSize <= getLargeBlobThresholdInBytes()) {
                executeSingleUpload(blobStore, buildKey(blobName), inputStream, blobSize, metadata, cryptoMetadata);
            } else {
                executeMultipartUpload(blobStore, buildKey(blobName), inputStream, blobSize, metadata, cryptoMetadata);
            }
        });
    }

    /**
     * Write blob with its object metadata.
     */
    @ExperimentalApi
    @Override
    public void writeBlobWithMetadata(
        String blobName,
        InputStream inputStream,
        long blobSize,
        boolean failIfAlreadyExists,
        @Nullable Map<String, String> metadata
    ) throws IOException {
        // Delegate to crypto-aware version with null CryptoMetadata
        writeBlobWithMetadata(blobName, inputStream, blobSize, failIfAlreadyExists, metadata, null);
    }

    /**
     * S3 evaluates {@code If-Match} and {@code If-None-Match} server-side and serializes conditional writes per key, so
     * the precondition holds against writers in any process and on any host - which is what makes this container usable
     * for remote store primary fencing.
     */
    @Override
    public boolean isConditionalWriteSupported() {
        return true;
    }

    @Override
    public VersionedBlob readBlobWithVersion(String blobName) throws IOException {
        // Versioned reads materialize the object in memory and are meant for small control blobs only. The ranged GET
        // bounds the transfer to the same limit the write side enforces (a ranged GET still returns the object's
        // ETag); an object at this key that exceeds the bound - misuse or corruption - is refused below instead of
        // being buffered whole.
        final long sizeBound = blobStore.bufferSizeInBytes();
        final GetObjectRequest getObjectRequest = GetObjectRequest.builder()
            .bucket(blobStore.bucket())
            .key(buildKey(blobName))
            .range("bytes=0-" + sizeBound)
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            final ResponseBytes<GetObjectResponse> responseBytes = AccessController.doPrivileged(
                () -> clientReference.get().getObjectAsBytes(getObjectRequest)
            );
            final byte[] content = responseBytes.asByteArray();
            if (content.length > sizeBound) {
                throw new IOException("[" + blobName + "] blob of size > [" + sizeBound + "] is too large for a versioned read");
            }
            return new VersionedBlob(content, responseBytes.response().eTag());
        } catch (NoSuchKeyException e) {
            throw new NoSuchFileException("[" + blobName + "] blob not found");
        } catch (SdkException e) {
            throw new IOException("Unable to read object [" + blobName + "] with version", e);
        }
    }

    @Override
    public String writeBlobConditionally(String blobName, InputStream inputStream, long blobSize, @Nullable String expectedVersionToken)
        throws IOException {
        if (blobSize > blobStore.bufferSizeInBytes()) {
            throw new IllegalArgumentException("Conditional write request size [" + blobSize + "] can't be larger than buffer size");
        }
        PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder()
            .bucket(blobStore.bucket())
            .key(buildKey(blobName))
            .contentLength(blobSize)
            .storageClass(blobStore.getStorageClass())
            .acl(blobStore.getCannedACL())
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().putObjectMetricPublisher))
            .expectedBucketOwner(blobStore.expectedBucketOwner());
        if (expectedVersionToken == null) {
            // create-if-absent
            putObjectRequestBuilder.ifNoneMatch("*");
        } else {
            // compare-and-swap on the version token (ETag) observed at read time
            putObjectRequestBuilder.ifMatch(expectedVersionToken);
        }
        configureEncryptionSettings(putObjectRequestBuilder, blobStore, null);
        final PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            final PutObjectResponse response = AccessController.doPrivileged(
                () -> clientReference.get().putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, blobSize))
            );
            return response.eTag();
        } catch (S3Exception e) {
            if (isConditionFailure(e, expectedVersionToken != null)) {
                throw new BlobVersionConflictException(
                    "conditional write conflict for blob [" + blobName + "]: expected [" + expectedVersionToken + "]",
                    e
                );
            }
            throw new IOException("Unable to conditionally upload object [" + blobName + "]", e);
        } catch (SdkException e) {
            throw new IOException("Unable to conditionally upload object [" + blobName + "]", e);
        }
    }

    /**
     * Distinguishes a genuinely lost compare-and-swap from a transient conflict. Losing the CAS is fatal for a fenced
     * writer, so only errors that prove the precondition was evaluated and NOT MET are classified as a lost CAS:
     * a 412, with or without an error code, a 409 carrying {@code PreconditionFailed}, or - for an {@code If-Match}
     * write only - a 404 carrying {@code NoSuchKey}. S3 fails an {@code If-Match} write with 404 rather than 412 when
     * the key has no current version at all (deleted, or a delete marker): the version the caller presented provably
     * no longer exists, so the precondition can never be met and retrying cannot change the answer. That is exactly
     * what a swept acknowledgement path looks like to a superseded writer, and it matches
     * {@code FsBlobContainer#writeBlobConditionally}, which also reports a conflict for a missing blob. A 404 without
     * {@code NoSuchKey} (e.g. {@code NoSuchBucket}) stays an ordinary {@link IOException}, and a create-if-absent
     * ({@code If-None-Match: *}) never 404s on the object itself. Every other 409 - including
     * {@code ConditionalRequestConflict}, which S3 returns when a concurrent conditional write on the same key is
     * still in flight and the outcome of this request's precondition is therefore UNKNOWN - must surface as an
     * ordinary {@link IOException} and be retried: the retry resolves it definitively (a 412 if the CAS was genuinely
     * lost, success if it was not), whereas classifying it as lost would fence a healthy writer whose rival lost.
     */
    private static boolean isConditionFailure(S3Exception e, boolean isCompareAndSwap) {
        // 412 Precondition Failed is unambiguous: the If-Match/If-None-Match condition was not met.
        if (e.statusCode() == 412) {
            return true;
        }
        final String errorCode = e.awsErrorDetails() == null ? null : e.awsErrorDetails().errorCode();
        if (isCompareAndSwap && e.statusCode() == 404) {
            // If-Match against a key with no current version. NoSuchKey is required: a 404 without it (NoSuchBucket,
            // or an unlabelled response) is not evidence about the precondition and stays retryable.
            return "NoSuchKey".equals(errorCode);
        }
        if (e.statusCode() != 409) {
            return false;
        }
        // Neither an unlabelled 409 nor a retryable conflict code is evidence that the precondition failed.
        return errorCode != null && CONDITIONAL_WRITE_CONFLICT_ERROR_CODES.contains(errorCode);
    }

    private static final Set<String> CONDITIONAL_WRITE_CONFLICT_ERROR_CODES = Set.of("PreconditionFailed");

    @Override
    public void asyncBlobUpload(WriteContext writeContext, ActionListener<Void> completionListener) throws IOException {
        CryptoMetadata crypto = writeContext.getCryptoMetadata();

        String indexKmsKey = null;
        String indexEncContext = null;

        if (crypto != null) {
            indexKmsKey = crypto.getKeyArn().orElse(null);
            indexEncContext = crypto.getEncryptionContext().orElse(null);
        }
        String mergeEncContext = SseKmsUtil.mergeAndEncodeEncryptionContexts(
            indexEncContext,
            blobStore.serverSideEncryptionEncryptionContext()
        );

        UploadRequest uploadRequest = new UploadRequest(
            blobStore.bucket(),
            buildKey(writeContext.getFileName()),
            writeContext.getFileSize(),
            writeContext.getWritePriority(),
            writeContext.getUploadFinalizer(),
            writeContext.doRemoteDataIntegrityCheck(),
            writeContext.getExpectedChecksum(),
            blobStore.isUploadRetryEnabled(),
            writeContext.getMetadata(),
            blobStore.serverSideEncryptionType(),
            indexKmsKey != null ? indexKmsKey : blobStore.serverSideEncryptionKmsKey(),
            blobStore.serverSideEncryptionBucketKey(),
            mergeEncContext,
            blobStore.expectedBucketOwner()
        );
        try {
            // If file size is greater than the queue capacity than SizeBasedBlockingQ will always reject the upload.
            // Therefore, redirecting it to slow client.
            if ((uploadRequest.getWritePriority() == WritePriority.LOW
                && blobStore.getLowPrioritySizeBasedBlockingQ().isMaxCapacityBelowContentLength(uploadRequest.getContentLength()) == false)
                || (uploadRequest.getWritePriority() != WritePriority.HIGH
                    && uploadRequest.getWritePriority() != WritePriority.URGENT
                    && blobStore.getNormalPrioritySizeBasedBlockingQ()
                        .isMaxCapacityBelowContentLength(uploadRequest.getContentLength()) == false)) {
                StreamContext streamContext = AccessController.doPrivileged(
                    () -> writeContext.getStreamProvider(uploadRequest.getContentLength())
                );
                InputStreamContainer inputStream = streamContext.provideStream(0);
                try {
                    executeMultipartUpload(
                        blobStore,
                        uploadRequest.getKey(),
                        inputStream.getInputStream(),
                        uploadRequest.getContentLength(),
                        uploadRequest.getMetadata(),
                        crypto
                    );
                    completionListener.onResponse(null);
                } catch (Exception ex) {
                    logger.error(
                        () -> new ParameterizedMessage(
                            "Failed to upload large file {} of size {} ",
                            uploadRequest.getKey(),
                            uploadRequest.getContentLength()
                        ),
                        ex
                    );
                    completionListener.onFailure(ex);
                }
                return;
            }
            long partSize = blobStore.getAsyncTransferManager()
                .calculateOptimalPartSize(writeContext.getFileSize(), writeContext.getWritePriority(), blobStore.isUploadRetryEnabled());
            StreamContext streamContext = AccessController.doPrivileged(() -> writeContext.getStreamProvider(partSize));
            try (AmazonAsyncS3Reference amazonS3Reference = AccessController.doPrivileged(blobStore::asyncClientReference)) {

                S3AsyncClient s3AsyncClient;
                if (writeContext.getWritePriority() == WritePriority.URGENT) {
                    s3AsyncClient = amazonS3Reference.get().urgentClient();
                } else if (writeContext.getWritePriority() == WritePriority.HIGH) {
                    s3AsyncClient = amazonS3Reference.get().priorityClient();
                } else {
                    s3AsyncClient = amazonS3Reference.get().client();
                }

                if (writeContext.getWritePriority() == WritePriority.URGENT
                    || writeContext.getWritePriority() == WritePriority.HIGH
                    || blobStore.isPermitBackedTransferEnabled() == false) {
                    createFileCompletableFuture(s3AsyncClient, uploadRequest, streamContext, completionListener);
                } else if (writeContext.getWritePriority() == WritePriority.LOW) {
                    blobStore.getLowPrioritySizeBasedBlockingQ()
                        .produce(
                            new SizeBasedBlockingQ.Item(
                                writeContext.getFileSize(),
                                () -> createFileCompletableFuture(s3AsyncClient, uploadRequest, streamContext, completionListener)
                            )
                        );
                } else if (writeContext.getWritePriority() == WritePriority.NORMAL) {
                    blobStore.getNormalPrioritySizeBasedBlockingQ()
                        .produce(
                            new SizeBasedBlockingQ.Item(
                                writeContext.getFileSize(),
                                () -> createFileCompletableFuture(s3AsyncClient, uploadRequest, streamContext, completionListener)
                            )
                        );
                } else {
                    throw new IllegalStateException("Cannot perform upload for other priority types.");
                }
            }
        } catch (Exception e) {
            logger.info("exception error from blob container for file {}", writeContext.getFileName());
            throw new IOException(e);
        }
    }

    private CompletableFuture<Void> createFileCompletableFuture(
        S3AsyncClient s3AsyncClient,
        UploadRequest uploadRequest,
        StreamContext streamContext,
        ActionListener<Void> completionListener
    ) {
        CompletableFuture<Void> completableFuture = blobStore.getAsyncTransferManager()
            .uploadObject(s3AsyncClient, uploadRequest, streamContext, blobStore.getStatsMetricPublisher());
        return completableFuture.whenComplete((response, throwable) -> {
            if (throwable == null) {
                completionListener.onResponse(response);
            } else {
                Exception ex = throwable instanceof Error ? new Exception(throwable) : (Exception) throwable;
                completionListener.onFailure(ex);
            }
        });
    }

    @ExperimentalApi
    @Override
    public void readBlobAsync(String blobName, ActionListener<ReadContext> listener) {
        try (AmazonAsyncS3Reference amazonS3Reference = AccessController.doPrivileged(blobStore::asyncClientReference)) {
            final S3AsyncClient s3AsyncClient = amazonS3Reference.get().client();
            final String bucketName = blobStore.bucket();
            final String blobKey = buildKey(blobName);

            final CompletableFuture<GetObjectAttributesResponse> blobMetadataFuture = getBlobMetadata(s3AsyncClient, bucketName, blobKey);

            blobMetadataFuture.whenComplete((blobMetadata, throwable) -> {
                if (throwable != null) {
                    Exception ex = throwable.getCause() instanceof Exception
                        ? (Exception) throwable.getCause()
                        : new Exception(throwable.getCause());
                    listener.onFailure(ex);
                    return;
                }

                try {
                    final List<ReadContext.StreamPartCreator> blobPartInputStreamFutures = new ArrayList<>();
                    final long blobSize = blobMetadata.objectSize();
                    final Integer numberOfParts = blobMetadata.objectParts() == null ? null : blobMetadata.objectParts().totalPartsCount();
                    final String blobChecksum = blobMetadata.checksum() == null ? null : blobMetadata.checksum().checksumCRC32();

                    if (numberOfParts == null) {
                        blobPartInputStreamFutures.add(() -> getBlobPartInputStreamContainer(s3AsyncClient, bucketName, blobKey, null));
                    } else {
                        // S3 multipart files use 1 to n indexing
                        for (int partNumber = 1; partNumber <= numberOfParts; partNumber++) {
                            final int innerPartNumber = partNumber;
                            blobPartInputStreamFutures.add(
                                () -> getBlobPartInputStreamContainer(s3AsyncClient, bucketName, blobKey, innerPartNumber)
                            );
                        }
                    }
                    listener.onResponse(new ReadContext.Builder(blobSize, blobPartInputStreamFutures).blobChecksum(blobChecksum).build());
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            });
        } catch (Exception ex) {
            listener.onFailure(SdkException.create("Error occurred while fetching blob parts from the repository", ex));
        }
    }

    public boolean remoteIntegrityCheckSupported() {
        return true;
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
        PlainActionFuture<DeleteResult> future = new PlainActionFuture<>();
        deleteAsync(future);
        return getFutureValue(future);
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        deleteBlobsAsyncIgnoringIfNotExists(blobNames, future);
        getFutureValue(future);
    }

    private <T> T getFutureValue(PlainActionFuture<T> future) throws IOException {
        try {
            return future.get(DEFAULT_OPERATION_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Future got interrupted", e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new RuntimeException(e.getCause());
        } catch (TimeoutException e) {
            FutureUtils.cancel(future);
            throw new IOException("Delete operation timed out after 30 seconds", e);
        }
    }

    @Override
    public List<BlobMetadata> listBlobsByPrefixInSortedOrder(String blobNamePrefix, int limit, BlobNameSortOrder blobNameSortOrder)
        throws IOException {
        // As AWS S3 returns list of keys in Lexicographic order, we don't have to fetch all the keys in order to sort them
        // We fetch only keys as per the given limit to optimize the fetch. If provided sort order is not Lexicographic,
        // we fall-back to default implementation of fetching all the keys and sorting them.
        if (blobNameSortOrder != BlobNameSortOrder.LEXICOGRAPHIC) {
            return super.listBlobsByPrefixInSortedOrder(blobNamePrefix, limit, blobNameSortOrder);
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
                return blobs.subList(0, Math.min(limit, blobs.size()));
            } catch (final Exception e) {
                throw new IOException("Exception when listing blobs by prefix [" + prefix + "]", e);
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
        return AccessController.doPrivileged(() -> {
            final List<ListObjectsV2Response> results = new ArrayList<>();
            int totalObjects = 0;
            ListObjectsV2Iterable listObjectsIterable = clientReference.get().listObjectsV2Paginator(listObjectsRequest);
            for (ListObjectsV2Response listObjectsV2Response : listObjectsIterable) {
                results.add(listObjectsV2Response);
                totalObjects += listObjectsV2Response.contents().size();
                if (limit != -1 && totalObjects >= limit) {
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
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .build();
    }

    private ListObjectsV2Request listObjectsRequest(String keyPath, int limit) {
        return listObjectsRequest(keyPath).toBuilder().maxKeys(Math.min(limit, 1000)).build();
    }

    private String buildKey(String blobName) {
        return keyPath + blobName;
    }

    @Override
    public long maxSingleRequestCopySizeInBytes() {
        return S3ServerSideCopy.MAX_SINGLE_COPY_SIZE.getBytes();
    }

    @Override
    public boolean isServerSideCopyConfigured() {
        // Gated on the destination side: without a granted access point there is no endpoint through which this
        // repository's identity can read a foreign source bucket.
        return Strings.hasText(blobStore.sourceAccessPoint());
    }

    /**
     * Describes this container as a server-side copy <em>source</em>.
     *
     * <p>Called on an existing service-owned repository - the automated snapshot repository or one of the remote store
     * repositories. Everything returned already exists on that repository: its bucket, its {@code base_path} as the
     * key prefix, and its {@code expected_bucket_owner} as the ownership guard. No source path is taken from the
     * caller.
     *
     * <p>Deliberately returns the <em>bucket</em>, not an access point. Which access point a given reader may use is a
     * property of that reader's grant, not of this bucket, so it is supplied by the destination.
     *
     * @throws IllegalStateException if this repository has {@code source_access_point} set, which would mean it is
     *                               configured as a copy destination and is being misused as a source
     */
    @Override
    public ServerSideCopySource asCopySource() {
        final String accessPoint = blobStore.sourceAccessPoint();
        if (Strings.hasText(accessPoint)) {
            // Only the destination may carry this setting. On a source it is silently ignored, so a repository that
            // declares one is either mis-registered or being used in the wrong role - either way, fail loudly rather
            // than copy from somewhere the operator did not intend.
            throw new IllegalStateException(
                "repository ["
                    + blobStore.repositoryMetadata().name()
                    + "] cannot be used as a server-side copy source because it declares ["
                    + S3Repository.SOURCE_ACCESS_POINT_SETTING.getKey()
                    + "="
                    + accessPoint
                    + "]. That setting identifies the access point a *destination* reads through and must be set only"
                    + " on the destination repository; remove it from this one."
            );
        }
        return new ServerSideCopySource(blobStore.bucket(), keyPath, blobStore.expectedBucketOwner());
    }

    /**
     * Pulls a blob from {@code source} into this container without the bytes entering this process.
     *
     * <p>Each input comes from exactly one place:
     * <ul>
     *   <li><b>Source</b> path and ownership guard come from {@code source}, which an existing service-owned
     *       repository produced via {@link #asCopySource()}.</li>
     *   <li><b>Source endpoint</b> is this repository's configured access point, which substitutes for the source
     *       bucket. It lives here because it is a property of this repository's grant rather than of the source
     *       bucket - with per-access-point role limits, two repositories reading the same bucket can be granted on
     *       different access points.</li>
     *   <li><b>Destination</b> bucket, path and ownership guard are this container's own.</li>
     * </ul>
     *
     * <p>Signed by this repository's configured identity. A server-side copy is signed by exactly one principal that
     * must hold read on the source and write on the destination simultaneously, and only the destination's owner can
     * grant the write half - so the copy is driven from here, not from the source.
     */
    @Override
    public void copyBlobFrom(final ServerSideCopySource source, final String sourceBlobName, final String destinationBlobName)
        throws IOException {
        if (isServerSideCopyConfigured() == false) {
            throw new UnsupportedOperationException(
                "server-side copy requires [" + S3Repository.SOURCE_ACCESS_POINT_SETTING.getKey() + "] on this repository"
            );
        }

        // The access point replaces the source bucket as the endpoint. This is what subjects the read to the access
        // point policy; addressing source.container() directly would bypass its prefix confinement.
        final String sourceEndpoint = blobStore.sourceAccessPoint();
        final String sourceKey = source.resolveKey(sourceBlobName);
        final String destinationKey = buildKey(destinationBlobName);

        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            // Size decides single-request versus multipart. Read through the same endpoint the copy will use, so an
            // access point policy that forbids the object fails here rather than part-way through a multipart copy.
            final long sourceSize = sourceObjectSize(clientReference.get(), sourceEndpoint, sourceKey, source.expectedContainerOwner());

            S3ServerSideCopy.copy(
                clientReference.get(),
                sourceEndpoint,
                sourceKey,
                sourceSize,
                source.expectedContainerOwner(),
                blobStore.bucket(),
                destinationKey,
                blobStore.expectedBucketOwner(),
                blobStore.bufferSizeInBytes()
            );
        }
    }

    /**
     * Size of the source object, read through {@code sourceEndpoint} rather than through the source's own container.
     *
     * <p>The substitution is the point, not an oversight. An access point stands in wherever a bucket name would go,
     * and addressing the bucket directly would bypass the access point policy that confines the read to a prefix.
     * {@link ServerSideCopySource#container()} is therefore deliberately unused here and in the copy itself; what is
     * taken from the source is its key prefix and its expected owner. The AWS SDK accepts an access point ARN in
     * place of a bucket name without extra client configuration.
     */
    private long sourceObjectSize(
        final S3Client client,
        final String sourceEndpoint,
        final String sourceKey,
        @Nullable final String sourceExpectedOwner
    ) throws IOException {
        final HeadObjectRequest.Builder request = HeadObjectRequest.builder().bucket(sourceEndpoint).key(sourceKey);
        if (Strings.hasText(sourceExpectedOwner)) {
            request.expectedBucketOwner(sourceExpectedOwner);
        }
        try {
            return AccessController.doPrivileged(() -> client.headObject(request.build())).contentLength();
        } catch (final NoSuchKeyException e) {
            final NoSuchFileException nsfe = new NoSuchFileException(sourceKey);
            nsfe.initCause(e);
            throw nsfe;
        } catch (final Exception e) {
            throw new IOException("failed to read size of copy source [" + sourceEndpoint + "/" + sourceKey + "]", e);
        }
    }

    /**
     * Uploads a blob using a single upload request
     */
    void executeSingleUpload(
        final S3BlobStore blobStore,
        final String blobName,
        final InputStream input,
        final long blobSize,
        final Map<String, String> metadata,
        @Nullable CryptoMetadata cryptoMetadata
    ) throws IOException {

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
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().putObjectMetricPublisher))
            .expectedBucketOwner(blobStore.expectedBucketOwner());

        if (CollectionUtils.isNotEmpty(metadata)) {
            putObjectRequestBuilder = putObjectRequestBuilder.metadata(metadata);
        }

        configureEncryptionSettings(putObjectRequestBuilder, blobStore, cryptoMetadata);

        PutObjectRequest putObjectRequest = putObjectRequestBuilder.build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            final InputStream requestInputStream;
            if (blobStore.isUploadRetryEnabled()) {
                requestInputStream = new BufferedInputStream(input, (int) (blobSize + 1));
            } else {
                requestInputStream = input;
            }
            AccessController.doPrivileged(
                () -> clientReference.get().putObject(putObjectRequest, RequestBody.fromInputStream(requestInputStream, blobSize))
            );
        } catch (final SdkException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using a single upload", e);
        }
    }

    /**
     * Uploads a blob using multipart upload requests.
     */
    void executeMultipartUpload(
        final S3BlobStore blobStore,
        final String blobName,
        final InputStream input,
        final long blobSize,
        final Map<String, String> metadata,
        @Nullable CryptoMetadata cryptoMetadata
    ) throws IOException {

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
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().multipartUploadMetricCollector))
            .expectedBucketOwner(blobStore.expectedBucketOwner());

        if (CollectionUtils.isNotEmpty(metadata)) {
            createMultipartUploadRequestBuilder.metadata(metadata);
        }

        configureEncryptionSettings(createMultipartUploadRequestBuilder, blobStore, cryptoMetadata);

        final InputStream requestInputStream;
        if (blobStore.isUploadRetryEnabled()) {
            requestInputStream = new BufferedInputStream(input, (int) (partSize + 1));
        } else {
            requestInputStream = input;
        }

        CreateMultipartUploadRequest createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            uploadId.set(
                AccessController.doPrivileged(() -> clientReference.get().createMultipartUpload(createMultipartUploadRequest).uploadId())
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
                    .expectedBucketOwner(blobStore.expectedBucketOwner())
                    .build();

                bytesCount += uploadPartRequest.contentLength();
                final UploadPartResponse uploadResponse = AccessController.doPrivileged(
                    () -> clientReference.get()
                        .uploadPart(uploadPartRequest, RequestBody.fromInputStream(requestInputStream, uploadPartRequest.contentLength()))
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
                .expectedBucketOwner(blobStore.expectedBucketOwner())
                .build();

            AccessController.doPrivileged(() -> clientReference.get().completeMultipartUpload(completeMultipartUploadRequest));
            success = true;

        } catch (final SdkException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } finally {
            if ((success == false) && Strings.hasLength(uploadId.get())) {
                AbortMultipartUploadRequest abortRequest = AbortMultipartUploadRequest.builder()
                    .bucket(bucketName)
                    .key(blobName)
                    .uploadId(uploadId.get())
                    .expectedBucketOwner(blobStore.expectedBucketOwner())
                    .build();
                try (AmazonS3Reference clientReference = blobStore.clientReference()) {
                    AccessController.doPrivileged(() -> clientReference.get().abortMultipartUpload(abortRequest));
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

    /**
     * Fetches a part of the blob from the S3 bucket and transforms it to an {@link InputStreamContainer}, which holds
     * the stream and its related metadata.
     * @param s3AsyncClient Async client to be utilized to fetch the object part
     * @param bucketName Name of the S3 bucket
     * @param blobKey Identifier of the blob for which the parts will be fetched
     * @param partNumber Optional part number for the blob to be retrieved
     * @return A future of {@link InputStreamContainer} containing the stream and stream metadata.
     */
    CompletableFuture<InputStreamContainer> getBlobPartInputStreamContainer(
        S3AsyncClient s3AsyncClient,
        String bucketName,
        String blobKey,
        @Nullable Integer partNumber
    ) {
        final boolean isMultipartObject = partNumber != null;
        final GetObjectRequest.Builder getObjectRequestBuilder = GetObjectRequest.builder()
            .bucket(bucketName)
            .key(blobKey)
            .expectedBucketOwner(blobStore.expectedBucketOwner());

        if (isMultipartObject) {
            getObjectRequestBuilder.partNumber(partNumber);
        }
        return AccessController.doPrivileged(
            () -> s3AsyncClient.getObject(getObjectRequestBuilder.build(), AsyncResponseTransformer.toBlockingInputStream())
                .thenApply(response -> transformResponseToInputStreamContainer(response, isMultipartObject))
        );
    }

    /**
     * Transforms the stream response object from S3 into an {@link InputStreamContainer}
     * @param streamResponse Response stream object from S3
     * @param isMultipartObject Flag to denote a multipart object response
     * @return {@link InputStreamContainer} containing the stream and stream metadata
     */
    // Package-Private for testing.
    static InputStreamContainer transformResponseToInputStreamContainer(
        ResponseInputStream<GetObjectResponse> streamResponse,
        boolean isMultipartObject
    ) {
        final GetObjectResponse getObjectResponse = streamResponse.response();
        final String contentRange = getObjectResponse.contentRange();
        final Long contentLength = getObjectResponse.contentLength();
        if ((isMultipartObject && contentRange == null) || contentLength == null) {
            throw SdkException.builder().message("Failed to fetch required metadata for blob part").build();
        }
        final long offset = isMultipartObject ? HttpRangeUtils.getStartOffsetFromRangeHeader(getObjectResponse.contentRange()) : 0L;
        return new InputStreamContainer(streamResponse, getObjectResponse.contentLength(), offset);
    }

    /**
     * Retrieves the metadata like checksum, object size and parts for the provided blob within the S3 bucket.
     * @param s3AsyncClient Async client to be utilized to fetch the metadata
     * @param bucketName Name of the S3 bucket
     * @param blobName Identifier of the blob for which the metadata will be fetched
     * @return A future containing the metadata within {@link GetObjectAttributesResponse}
     */
    CompletableFuture<GetObjectAttributesResponse> getBlobMetadata(S3AsyncClient s3AsyncClient, String bucketName, String blobName) {
        // Fetch blob metadata - part info, size, checksum
        final GetObjectAttributesRequest getObjectAttributesRequest = GetObjectAttributesRequest.builder()
            .bucket(bucketName)
            .key(blobName)
            .objectAttributes(ObjectAttributes.CHECKSUM, ObjectAttributes.OBJECT_SIZE, ObjectAttributes.OBJECT_PARTS)
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .build();

        return AccessController.doPrivileged(() -> s3AsyncClient.getObjectAttributes(getObjectAttributesRequest));
    }

    @Override
    public void deleteAsync(ActionListener<DeleteResult> completionListener) {
        logger.debug("Starting async deletion for path [{}]", keyPath);
        try (AmazonAsyncS3Reference asyncClientReference = blobStore.asyncClientReference()) {
            S3AsyncClient s3AsyncClient = asyncClientReference.get().client();

            ListObjectsV2Request listRequest = ListObjectsV2Request.builder()
                .bucket(blobStore.bucket())
                .prefix(keyPath)
                .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().listObjectsMetricPublisher))
                .build();
            ListObjectsV2Publisher listPublisher = s3AsyncClient.listObjectsV2Paginator(listRequest);

            AtomicLong deletedBlobs = new AtomicLong();
            AtomicLong deletedBytes = new AtomicLong();

            CompletableFuture<Void> listingFuture = new CompletableFuture<>();

            listPublisher.subscribe(new Subscriber<>() {
                private Subscription subscription;
                private final List<String> objectsToDelete = new ArrayList<>();
                private CompletableFuture<Void> deletionChain = CompletableFuture.completedFuture(null);

                @Override
                public void onSubscribe(Subscription s) {
                    this.subscription = s;
                    logger.debug("Subscribed to list objects publisher for path [{}]", keyPath);
                    subscription.request(1);
                }

                @Override
                public void onNext(ListObjectsV2Response response) {
                    response.contents().forEach(s3Object -> {
                        deletedBlobs.incrementAndGet();
                        deletedBytes.addAndGet(s3Object.size());
                        objectsToDelete.add(s3Object.key());
                    });

                    logger.debug("Found {} objects to delete in current batch for path [{}]", response.contents().size(), keyPath);

                    int bulkDeleteSize = blobStore.getBulkDeletesSize();
                    if (objectsToDelete.size() >= bulkDeleteSize) {
                        int fullBatchesCount = objectsToDelete.size() / bulkDeleteSize;
                        int itemsToDelete = fullBatchesCount * bulkDeleteSize;

                        List<String> batchToDelete = new ArrayList<>(objectsToDelete.subList(0, itemsToDelete));
                        objectsToDelete.subList(0, itemsToDelete).clear();

                        logger.debug("Executing bulk delete of {} objects for path [{}]", batchToDelete.size(), keyPath);
                        deletionChain = S3AsyncDeleteHelper.executeDeleteChain(
                            s3AsyncClient,
                            blobStore,
                            batchToDelete,
                            deletionChain,
                            () -> subscription.request(1)
                        );
                    } else {
                        subscription.request(1);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    logger.error(() -> new ParameterizedMessage("Failed to list objects for deletion in path [{}]", keyPath), t);
                    listingFuture.completeExceptionally(new IOException("Failed to list objects for deletion", t));
                }

                @Override
                public void onComplete() {
                    logger.debug(
                        "Completed listing objects for path [{}], remaining objects to delete: {}",
                        keyPath,
                        objectsToDelete.size()
                    );
                    if (!objectsToDelete.isEmpty()) {
                        logger.debug("Executing final bulk delete of {} objects for path [{}]", objectsToDelete.size(), keyPath);
                        deletionChain = S3AsyncDeleteHelper.executeDeleteChain(
                            s3AsyncClient,
                            blobStore,
                            objectsToDelete,
                            deletionChain,
                            null
                        );
                    }
                    deletionChain.whenComplete((v, throwable) -> {
                        if (throwable != null) {
                            logger.error(
                                () -> new ParameterizedMessage("Failed to complete deletion chain for path [{}]", keyPath),
                                throwable
                            );
                            listingFuture.completeExceptionally(throwable);
                        } else {
                            logger.debug("Successfully completed deletion chain for path [{}]", keyPath);
                            listingFuture.complete(null);
                        }
                    });
                }
            });

            listingFuture.whenComplete((v, throwable) -> {
                if (throwable != null) {
                    logger.error(() -> new ParameterizedMessage("Failed to complete async deletion for path [{}]", keyPath), throwable);
                    completionListener.onFailure(
                        throwable instanceof Exception e ? e : new IOException("Unexpected error during async deletion", throwable)
                    );
                } else {
                    logger.debug(
                        "Successfully completed async deletion for path [{}]. Deleted {} blobs totaling {} bytes",
                        keyPath,
                        deletedBlobs.get(),
                        deletedBytes.get()
                    );
                    completionListener.onResponse(new DeleteResult(deletedBlobs.get(), deletedBytes.get()));
                }
            });
        } catch (Exception e) {
            logger.error(() -> new ParameterizedMessage("Failed to initiate async deletion for path [{}]", keyPath), e);
            completionListener.onFailure(new IOException("Failed to initiate async deletion", e));
        }
    }

    @Override
    public void deleteBlobsAsyncIgnoringIfNotExists(List<String> blobNames, ActionListener<Void> completionListener) {
        if (blobNames.isEmpty()) {
            completionListener.onResponse(null);
            return;
        }

        try (AmazonAsyncS3Reference asyncClientReference = blobStore.asyncClientReference()) {
            S3AsyncClient s3AsyncClient = asyncClientReference.get().client();

            List<String> keysToDelete = blobNames.stream().map(this::buildKey).collect(Collectors.toList());

            S3AsyncDeleteHelper.executeDeleteChain(s3AsyncClient, blobStore, keysToDelete, CompletableFuture.completedFuture(null), null)
                .whenComplete((v, throwable) -> {
                    if (throwable != null) {
                        completionListener.onFailure(new IOException("Failed to delete blobs " + blobNames, throwable));
                    } else {
                        completionListener.onResponse(null);
                    }
                });
        } catch (Exception e) {
            completionListener.onFailure(new IOException("Failed to initiate async blob deletion", e));
        }
    }
}
