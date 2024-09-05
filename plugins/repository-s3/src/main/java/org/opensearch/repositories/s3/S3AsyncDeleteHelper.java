/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3;

import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

public class S3AsyncDeleteHelper {
    private static final Logger logger = LogManager.getLogger(S3AsyncDeleteHelper.class);

    static CompletableFuture<Void> executeDeleteChain(
        S3AsyncClient s3AsyncClient,
        S3BlobStore blobStore,
        List<String> objectsToDelete,
        CompletableFuture<Void> currentChain,
        boolean ignoreIfNotExists,
        Runnable afterDeleteAction
    ) {
        List<List<String>> batches = createDeleteBatches(objectsToDelete, blobStore.getBulkDeletesSize());
        CompletableFuture<Void> newChain = currentChain.thenCompose(
            v -> executeDeleteBatches(s3AsyncClient, blobStore, batches, ignoreIfNotExists)
        );
        if (afterDeleteAction != null) {
            newChain = newChain.thenRun(afterDeleteAction);
        }
        return newChain;
    }

    static List<List<String>> createDeleteBatches(List<String> keys, int bulkDeleteSize) {
        List<List<String>> batches = new ArrayList<>();
        for (int i = 0; i < keys.size(); i += bulkDeleteSize) {
            batches.add(keys.subList(i, Math.min(keys.size(), i + bulkDeleteSize)));
        }
        return batches;
    }

    private static CompletableFuture<Void> executeDeleteBatches(
        S3AsyncClient s3AsyncClient,
        S3BlobStore blobStore,
        List<List<String>> batches,
        boolean ignoreIfNotExists
    ) {
        CompletableFuture<Void> allDeletesFuture = CompletableFuture.completedFuture(null);

        for (List<String> batch : batches) {
            allDeletesFuture = allDeletesFuture.thenCompose(
                v -> executeSingleDeleteBatch(s3AsyncClient, blobStore, batch, ignoreIfNotExists)
            );
        }

        return allDeletesFuture;
    }

    private static CompletableFuture<Void> executeSingleDeleteBatch(
        S3AsyncClient s3AsyncClient,
        S3BlobStore blobStore,
        List<String> batch,
        boolean ignoreIfNotExists
    ) {
        DeleteObjectsRequest deleteRequest = bulkDelete(blobStore.bucket(), batch, blobStore);
        return s3AsyncClient.deleteObjects(deleteRequest)
            .thenApply(response -> processDeleteResponse(response, ignoreIfNotExists))
            .exceptionally(e -> {
                if (!ignoreIfNotExists) {
                    throw new CompletionException(e);
                }
                logger.warn("Error during batch deletion", e);
                return null;
            });
    }

    private static Void processDeleteResponse(DeleteObjectsResponse deleteObjectsResponse, boolean ignoreIfNotExists) {
        if (!deleteObjectsResponse.errors().isEmpty()) {
            if (ignoreIfNotExists) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Failed to delete some blobs {}",
                        deleteObjectsResponse.errors()
                            .stream()
                            .map(s3Error -> "[" + s3Error.key() + "][" + s3Error.code() + "][" + s3Error.message() + "]")
                            .collect(Collectors.toList())
                    )
                );
            } else {
                throw new CompletionException(new IOException("Failed to delete some blobs: " + deleteObjectsResponse.errors()));
            }
        }
        return null;
    }

    private static DeleteObjectsRequest bulkDelete(String bucket, List<String> blobs, S3BlobStore blobStore) {
        return DeleteObjectsRequest.builder()
            .bucket(bucket)
            .delete(
                Delete.builder()
                    .objects(blobs.stream().map(blob -> ObjectIdentifier.builder().key(blob).build()).collect(Collectors.toList()))
                    .quiet(true)
                    .build()
            )
            .overrideConfiguration(o -> o.addMetricPublisher(blobStore.getStatsMetricPublisher().deleteObjectsMetricPublisher))
            .build();
    }
}
