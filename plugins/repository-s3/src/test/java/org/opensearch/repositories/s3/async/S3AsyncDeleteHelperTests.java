/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.s3.async;

import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;

import org.opensearch.repositories.s3.S3BlobStore;
import org.opensearch.repositories.s3.StatsMetricPublisher;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class S3AsyncDeleteHelperTests extends OpenSearchTestCase {

    @Mock
    private S3AsyncClient s3AsyncClient;

    @Mock
    private S3BlobStore blobStore;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MockitoAnnotations.openMocks(this);
    }

    public void testExecuteDeleteChain() {
        List<String> objectsToDelete = Arrays.asList("key1", "key2", "key3");
        CompletableFuture<Void> currentChain = CompletableFuture.completedFuture(null);

        // Mock the deleteObjects method of S3AsyncClient
        when(s3AsyncClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(
            CompletableFuture.completedFuture(DeleteObjectsResponse.builder().build())
        );

        // Mock the getBulkDeletesSize method of S3BlobStore
        when(blobStore.getBulkDeletesSize()).thenReturn(2);

        // Mock the getStatsMetricPublisher method of S3BlobStore to return a non-null value
        StatsMetricPublisher mockMetricPublisher = mock(StatsMetricPublisher.class);
        MetricPublisher mockDeleteObjectsMetricPublisher = mock(MetricPublisher.class);
        when(blobStore.getStatsMetricPublisher()).thenReturn(mockMetricPublisher);
        when(mockMetricPublisher.getDeleteObjectsMetricPublisher()).thenReturn(mockDeleteObjectsMetricPublisher);

        CompletableFuture<Void> newChain = S3AsyncDeleteHelper.executeDeleteChain(
            s3AsyncClient,
            blobStore,
            objectsToDelete,
            currentChain,
            null
        );

        // Verify that the newChain is completed without any exceptions
        assertNotNull(newChain);
        assertTrue(newChain.isDone());
        assertFalse(newChain.isCompletedExceptionally());

        // Verify that the deleteObjects method of S3AsyncClient was called with the expected request
        ArgumentCaptor<DeleteObjectsRequest> requestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(s3AsyncClient, times(2)).deleteObjects(requestCaptor.capture());

        List<DeleteObjectsRequest> capturedRequests = requestCaptor.getAllValues();
        assertEquals(2, capturedRequests.size());

        // Verify that the requests have the expected metric publisher added
        for (DeleteObjectsRequest request : capturedRequests) {
            assertNotNull(request.overrideConfiguration());
            assertTrue(request.overrideConfiguration().get().metricPublishers().contains(mockDeleteObjectsMetricPublisher));
        }
    }

    public void testCreateDeleteBatches() {
        List<String> keys = Arrays.asList("key1", "key2", "key3", "key4", "key5", "key6");
        int bulkDeleteSize = 3;

        List<List<String>> batches = S3AsyncDeleteHelper.createDeleteBatches(keys, bulkDeleteSize);

        assertEquals(2, batches.size());
        assertEquals(Arrays.asList("key1", "key2", "key3"), batches.get(0));
        assertEquals(Arrays.asList("key4", "key5", "key6"), batches.get(1));
    }

    public void testExecuteSingleDeleteBatch() throws Exception {
        List<String> batch = Arrays.asList("key1", "key2");
        DeleteObjectsResponse expectedResponse = DeleteObjectsResponse.builder().build();

        when(s3AsyncClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        // Mock the getStatsMetricPublisher method of S3BlobStore to return a non-null value
        StatsMetricPublisher mockMetricPublisher = mock(StatsMetricPublisher.class);
        MetricPublisher mockDeleteObjectsMetricPublisher = mock(MetricPublisher.class);
        when(blobStore.getStatsMetricPublisher()).thenReturn(mockMetricPublisher);
        when(mockMetricPublisher.getDeleteObjectsMetricPublisher()).thenReturn(mockDeleteObjectsMetricPublisher);

        CompletableFuture<Void> future = S3AsyncDeleteHelper.executeSingleDeleteBatch(s3AsyncClient, blobStore, batch);

        assertNotNull(future);
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        future.join(); // Wait for the CompletableFuture to complete

        // Verify that the deleteObjects method of S3AsyncClient was called with the expected request
        ArgumentCaptor<DeleteObjectsRequest> requestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(s3AsyncClient).deleteObjects(requestCaptor.capture());

        DeleteObjectsRequest capturedRequest = requestCaptor.getValue();
        assertEquals(blobStore.bucket(), capturedRequest.bucket());
        assertEquals(batch.size(), capturedRequest.delete().objects().size());
        assertTrue(capturedRequest.delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toList()).containsAll(batch));
    }

    public void testProcessDeleteResponse() {
        DeleteObjectsResponse response = DeleteObjectsResponse.builder()
            .errors(
                Arrays.asList(
                    S3Error.builder().key("key1").code("Code1").message("Message1").build(),
                    S3Error.builder().key("key2").code("Code2").message("Message2").build()
                )
            )
            .build();

        // Call the processDeleteResponse method
        S3AsyncDeleteHelper.processDeleteResponse(response);
    }

    public void testBulkDelete() {
        List<String> blobs = Arrays.asList("key1", "key2", "key3");
        String bucketName = "my-bucket";

        // Mock the getStatsMetricPublisher method of S3BlobStore to return a non-null value
        StatsMetricPublisher mockMetricPublisher = mock(StatsMetricPublisher.class);
        MetricPublisher mockDeleteObjectsMetricPublisher = mock(MetricPublisher.class);
        when(blobStore.getStatsMetricPublisher()).thenReturn(mockMetricPublisher);
        when(mockMetricPublisher.getDeleteObjectsMetricPublisher()).thenReturn(mockDeleteObjectsMetricPublisher);

        DeleteObjectsRequest request = S3AsyncDeleteHelper.bulkDelete(bucketName, blobs, blobStore);

        assertEquals(bucketName, request.bucket());
        assertEquals(blobs.size(), request.delete().objects().size());
        assertTrue(request.delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toList()).containsAll(blobs));
        assertNotNull(request.overrideConfiguration());
        assertTrue(request.overrideConfiguration().get().metricPublishers().contains(mockDeleteObjectsMetricPublisher));
    }

    public void testExecuteDeleteBatches() {
        List<List<String>> batches = Arrays.asList(Arrays.asList("key1", "key2"), Arrays.asList("key3", "key4"));
        DeleteObjectsResponse expectedResponse = DeleteObjectsResponse.builder().build();

        when(s3AsyncClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(CompletableFuture.completedFuture(expectedResponse));

        // Mock the getStatsMetricPublisher method of S3BlobStore to return a non-null value
        StatsMetricPublisher mockMetricPublisher = mock(StatsMetricPublisher.class);
        MetricPublisher mockDeleteObjectsMetricPublisher = mock(MetricPublisher.class);
        when(blobStore.getStatsMetricPublisher()).thenReturn(mockMetricPublisher);
        when(mockMetricPublisher.getDeleteObjectsMetricPublisher()).thenReturn(mockDeleteObjectsMetricPublisher);

        CompletableFuture<Void> future = S3AsyncDeleteHelper.executeDeleteBatches(s3AsyncClient, blobStore, batches);

        assertNotNull(future);
        assertTrue(future.isDone());
        assertFalse(future.isCompletedExceptionally());
        future.join(); // Wait for the CompletableFuture to complete

        // Verify that the deleteObjects method of S3AsyncClient was called with the expected requests
        ArgumentCaptor<DeleteObjectsRequest> requestCaptor = ArgumentCaptor.forClass(DeleteObjectsRequest.class);
        verify(s3AsyncClient, times(2)).deleteObjects(requestCaptor.capture());

        List<DeleteObjectsRequest> capturedRequests = requestCaptor.getAllValues();
        assertEquals(2, capturedRequests.size());
        for (DeleteObjectsRequest request : capturedRequests) {
            assertNotNull(request.overrideConfiguration());
            assertTrue(request.overrideConfiguration().get().metricPublishers().contains(mockDeleteObjectsMetricPublisher));
        }
    }

    public void testExecuteDeleteChainWithAfterDeleteAction() {
        List<String> objectsToDelete = Arrays.asList("key1", "key2", "key3");
        CompletableFuture<Void> currentChain = CompletableFuture.completedFuture(null);
        Runnable afterDeleteAction = mock(Runnable.class);

        // Mock the deleteObjects method of S3AsyncClient
        when(s3AsyncClient.deleteObjects(any(DeleteObjectsRequest.class))).thenReturn(
            CompletableFuture.completedFuture(DeleteObjectsResponse.builder().build())
        );

        // Mock the getBulkDeletesSize method of S3BlobStore
        when(blobStore.getBulkDeletesSize()).thenReturn(2);

        // Mock the getStatsMetricPublisher method of S3BlobStore to return a non-null value
        StatsMetricPublisher mockMetricPublisher = mock(StatsMetricPublisher.class);
        MetricPublisher mockDeleteObjectsMetricPublisher = mock(MetricPublisher.class);
        when(blobStore.getStatsMetricPublisher()).thenReturn(mockMetricPublisher);
        when(mockMetricPublisher.getDeleteObjectsMetricPublisher()).thenReturn(mockDeleteObjectsMetricPublisher);

        CompletableFuture<Void> newChain = S3AsyncDeleteHelper.executeDeleteChain(
            s3AsyncClient,
            blobStore,
            objectsToDelete,
            currentChain,
            afterDeleteAction
        );

        // Verify that the newChain is completed without any exceptions
        assertNotNull(newChain);
        assertTrue(newChain.isDone());
        assertFalse(newChain.isCompletedExceptionally());

        // Verify that the afterDeleteAction was called
        verify(afterDeleteAction).run();
    }

}
