/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.repositories.s3.S3BlobStore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class S3AsyncDeleteHelper {
    private static final Logger logger = LogManager.getLogger(S3AsyncDeleteHelper.class);

    public static CompletableFuture<Void> executeDeleteChain(
        S3AsyncClient s3AsyncClient,
        S3BlobStore blobStore,
        List<String> objectsToDelete,
        CompletableFuture<Void> currentChain,
        Runnable afterDeleteAction
    ) {
        logger.debug("Starting delete chain execution for {} objects", objectsToDelete.size());
        List<List<String>> batches = createDeleteBatches(objectsToDelete, blobStore.getBulkDeletesSize());
        logger.debug("Created {} delete batches", batches.size());
        CompletableFuture<Void> newChain = currentChain.thenCompose(v -> executeDeleteBatches(s3AsyncClient, blobStore, batches));
        if (afterDeleteAction != null) {
            logger.debug("Adding post-delete action to the chain");
            newChain = newChain.thenRun(afterDeleteAction);
        }
        return newChain;
    }

    static List<List<String>> createDeleteBatches(List<String> keys, int bulkDeleteSize) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < keys.size(); i += bulkDeleteSize) {
            int batchSize = Math.min(keys.size() - i, bulkDeleteSize);
            batches.add(keys.subList(i, i + batchSize));
            logger.debug("Created delete batch of size {} starting at index {}", batchSize, i);
        }
        return batches;
    }

    static CompletableFuture<Void> executeDeleteBatches(S3AsyncClient s3AsyncClient, S3BlobStore blobStore, List<List<String>> batches) {
        logger.debug("Starting execution of {} delete batches", batches.size());
        CompletableFuture<Void> allDeletesFuture = CompletableFuture.completedFuture(null);

        for (int i = 0; i < batches.size(); i++) {
            List<String> batch = batches.get(i);
            logger.debug("Queueing batch {} of {} with {} objects", i + 1, batches.size(), batch.size());
            allDeletesFuture = allDeletesFuture.thenCompose(v -> executeSingleDeleteBatch(s3AsyncClient, blobStore, batch));
        }

        return allDeletesFuture.whenComplete((v, throwable) -> {
            if (throwable != null) {
                logger.error("Failed to complete delete batches execution", throwable);
            } else {
                logger.debug("Completed execution of all delete batches");
            }
        });
    }

    static CompletableFuture<Void> executeSingleDeleteBatch(S3AsyncClient s3AsyncClient, S3BlobStore blobStore, List<String> batch) {
        logger.debug("Executing delete batch of {} objects", batch.size());
        DeleteObjectsRequest deleteRequest = bulkDelete(blobStore.bucket(), batch, blobStore);
        CompletableFuture<DeleteObjectsResponse> deleteFuture = s3AsyncClient.deleteObjects(deleteRequest);

        if (deleteFuture == null) {
            logger.error("S3AsyncClient.deleteObjects returned null - client may not be properly initialized");
            return CompletableFuture.failedFuture(new IllegalStateException("S3AsyncClient returned null future"));
        }

        return deleteFuture.thenApply(response -> {
            logger.debug("Received delete response for batch of {} objects", batch.size());
            return processDeleteResponse(response);
        });
    }

    static Void processDeleteResponse(DeleteObjectsResponse deleteObjectsResponse) {
        if (deleteObjectsResponse.errors().isEmpty()) {
            logger.debug("Successfully processed delete response with no errors");
        } else {
            List<String> errorDetails = deleteObjectsResponse.errors()
                .stream()
                .map(s3Error -> "[" + s3Error.key() + "][" + s3Error.code() + "][" + s3Error.message() + "]")
                .collect(Collectors.toList());
            logger.warn(
                () -> new ParameterizedMessage("Failed to delete {} objects: {}", deleteObjectsResponse.errors().size(), errorDetails)
            );
        }
        return null;
    }

    static DeleteObjectsRequest bulkDelete(String bucket, List<String> blobs, S3BlobStore blobStore) {
        logger.debug("Creating bulk delete request for {} objects in bucket {}", blobs.size(), bucket);
        return DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(
                Delete.builder()
                    .objects(blobs.stream().map(blob -> ObjectIdentifier.builder().key(blob).build()).collect(Collectors.toList()))
                    .quiet(true)
                    .build()
            )
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().getDeleteObjectsMetricPublisher()))
            .expectedBucketOwner(blobStore.expectedBucketOwner())
            .build();
    }
}
