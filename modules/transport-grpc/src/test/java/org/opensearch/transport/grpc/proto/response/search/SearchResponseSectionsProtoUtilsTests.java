/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHits;
import org.opensearch.search.pipeline.ProcessorExecutionDetail;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchResponseSectionsProtoUtilsTests extends OpenSearchTestCase {

    public void testToProtoWithProcessorResults() throws IOException {
        // Create processor execution details
        List<ProcessorExecutionDetail> processorResults = new ArrayList<>();

        Map<String, Object> inputData1 = new HashMap<>();
        inputData1.put("key1", "value1");
        inputData1.put("key2", 42);

        Map<String, Object> outputData1 = new HashMap<>();
        outputData1.put("result", "success");

        ProcessorExecutionDetail detail1 = new ProcessorExecutionDetail(
            "processor1",
            100L,
            inputData1,
            outputData1,
            ProcessorExecutionDetail.ProcessorStatus.SUCCESS,
            null,
            "tag1"
        );
        processorResults.add(detail1);

        ProcessorExecutionDetail detail2 = new ProcessorExecutionDetail(
            "processor2",
            50L,
            null,
            null,
            ProcessorExecutionDetail.ProcessorStatus.FAIL,
            "Error message",
            null
        );
        processorResults.add(detail2);

        // Create mock SearchResponseSections
        SearchResponseSections mockSections = mock(SearchResponseSections.class);
        when(mockSections.getProcessorResult()).thenReturn(processorResults);
        when(mockSections.getSearchExtBuilders()).thenReturn(new ArrayList<>());

        // Create mock SearchResponse
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mockSections);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.getAggregations()).thenReturn(null);
        when(mockResponse.getSuggest()).thenReturn(null);
        when(mockResponse.getProfileResults()).thenReturn(null);

        // Call the method under test
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        SearchResponseSectionsProtoUtils.toProto(builder, mockResponse);
        org.opensearch.protobufs.SearchResponse protoResponse = builder.build();

        // Verify processor results
        assertNotNull("Proto response should not be null", protoResponse);
        assertEquals("Should have 2 processor results", 2, protoResponse.getProcessorResultsCount());

        org.opensearch.protobufs.ProcessorExecutionDetail protoDetail1 = protoResponse.getProcessorResults(0);
        assertEquals("Processor name should match", "processor1", protoDetail1.getProcessorName());
        assertEquals("Duration should match", 100L, protoDetail1.getDurationMillis());
        assertEquals("Tag should match", "tag1", protoDetail1.getTag());
        assertEquals("Status should match", "success", protoDetail1.getStatus());
        assertTrue("Should have input data", protoDetail1.hasInputData());
        assertTrue("Should have output data", protoDetail1.hasOutputData());
        assertFalse("Should not have error", protoDetail1.hasError());

        org.opensearch.protobufs.ProcessorExecutionDetail protoDetail2 = protoResponse.getProcessorResults(1);
        assertEquals("Processor name should match", "processor2", protoDetail2.getProcessorName());
        assertEquals("Duration should match", 50L, protoDetail2.getDurationMillis());
        assertEquals("Status should match", "fail", protoDetail2.getStatus());
        assertEquals("Error message should match", "Error message", protoDetail2.getError());
        assertFalse("Should not have tag", protoDetail2.hasTag());
    }

    public void testToProtoWithEmptyProcessorResults() throws IOException {
        // Create mock SearchResponseSections with empty processor results
        SearchResponseSections mockSections = mock(SearchResponseSections.class);
        when(mockSections.getProcessorResult()).thenReturn(new ArrayList<>());
        when(mockSections.getSearchExtBuilders()).thenReturn(new ArrayList<>());

        // Create mock SearchResponse
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mockSections);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));

        // Call the method under test
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        SearchResponseSectionsProtoUtils.toProto(builder, mockResponse);
        org.opensearch.protobufs.SearchResponse protoResponse = builder.build();

        // Verify no processor results
        assertEquals("Should have 0 processor results", 0, protoResponse.getProcessorResultsCount());
    }

    public void testToProtoWithNullProcessorResults() throws IOException {
        // Create mock SearchResponseSections with null processor results
        SearchResponseSections mockSections = mock(SearchResponseSections.class);
        when(mockSections.getProcessorResult()).thenReturn(null);
        when(mockSections.getSearchExtBuilders()).thenReturn(new ArrayList<>());

        // Create mock SearchResponse
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mockSections);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));

        // Call the method under test
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        SearchResponseSectionsProtoUtils.toProto(builder, mockResponse);
        org.opensearch.protobufs.SearchResponse protoResponse = builder.build();

        // Verify no processor results
        assertEquals("Should have 0 processor results", 0, protoResponse.getProcessorResultsCount());
    }

    public void testToProtoWithProcessorResultNullFields() throws IOException {
        // Create processor execution detail with null fields
        List<ProcessorExecutionDetail> processorResults = new ArrayList<>();
        ProcessorExecutionDetail detail = new ProcessorExecutionDetail(
            "processor1",
            100L,
            null,
            null,
            ProcessorExecutionDetail.ProcessorStatus.SUCCESS,
            null,
            null
        );
        processorResults.add(detail);

        // Create mock SearchResponseSections
        SearchResponseSections mockSections = mock(SearchResponseSections.class);
        when(mockSections.getProcessorResult()).thenReturn(processorResults);
        when(mockSections.getSearchExtBuilders()).thenReturn(new ArrayList<>());

        // Create mock SearchResponse
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mockSections);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.getAggregations()).thenReturn(null);
        when(mockResponse.getSuggest()).thenReturn(null);
        when(mockResponse.getProfileResults()).thenReturn(null);

        // Call the method under test
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        SearchResponseSectionsProtoUtils.toProto(builder, mockResponse);
        org.opensearch.protobufs.SearchResponse protoResponse = builder.build();

        // Verify processor result
        assertEquals("Should have 1 processor result", 1, protoResponse.getProcessorResultsCount());
        org.opensearch.protobufs.ProcessorExecutionDetail protoDetail = protoResponse.getProcessorResults(0);
        assertEquals("Processor name should match", "processor1", protoDetail.getProcessorName());
        assertEquals("Duration should match", 100L, protoDetail.getDurationMillis());
        assertEquals("Status should match", "success", protoDetail.getStatus());
        assertFalse("Should not have input data", protoDetail.hasInputData());
        assertFalse("Should not have output data", protoDetail.hasOutputData());
        assertFalse("Should not have error", protoDetail.hasError());
        assertFalse("Should not have tag", protoDetail.hasTag());
    }

    public void testToProtoWithProcessorResultNullProcessorName() throws IOException {
        // Create processor execution detail with null processor name
        List<ProcessorExecutionDetail> processorResults = new ArrayList<>();
        ProcessorExecutionDetail detail = new ProcessorExecutionDetail(
            null,
            100L,
            null,
            null,
            ProcessorExecutionDetail.ProcessorStatus.SUCCESS,
            null,
            null
        );
        processorResults.add(detail);

        // Create mock SearchResponseSections
        SearchResponseSections mockSections = mock(SearchResponseSections.class);
        when(mockSections.getProcessorResult()).thenReturn(processorResults);
        when(mockSections.getSearchExtBuilders()).thenReturn(new ArrayList<>());

        // Create mock SearchResponse
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mockSections);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.getAggregations()).thenReturn(null);
        when(mockResponse.getSuggest()).thenReturn(null);
        when(mockResponse.getProfileResults()).thenReturn(null);

        // Call the method under test
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        SearchResponseSectionsProtoUtils.toProto(builder, mockResponse);
        org.opensearch.protobufs.SearchResponse protoResponse = builder.build();

        // Verify processor result - null processor name should not be set
        assertEquals("Should have 1 processor result", 1, protoResponse.getProcessorResultsCount());
        org.opensearch.protobufs.ProcessorExecutionDetail protoDetail = protoResponse.getProcessorResults(0);
        assertEquals("Duration should match", 100L, protoDetail.getDurationMillis());
        assertFalse("Should not have processor name", protoDetail.hasProcessorName());
    }

    public void testToProtoWithProcessorResultNullStatus() throws IOException {
        // Create processor execution detail with null status
        List<ProcessorExecutionDetail> processorResults = new ArrayList<>();
        ProcessorExecutionDetail detail = new ProcessorExecutionDetail("processor1", 100L, null, null, null, null, null);
        processorResults.add(detail);

        // Create mock SearchResponseSections
        SearchResponseSections mockSections = mock(SearchResponseSections.class);
        when(mockSections.getProcessorResult()).thenReturn(processorResults);
        when(mockSections.getSearchExtBuilders()).thenReturn(new ArrayList<>());

        // Create mock SearchResponse
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mockSections);
        when(mockResponse.getTook()).thenReturn(TimeValue.timeValueMillis(100));
        when(mockResponse.isTimedOut()).thenReturn(false);
        when(mockResponse.getTotalShards()).thenReturn(5);
        when(mockResponse.getSuccessfulShards()).thenReturn(5);
        when(mockResponse.getSkippedShards()).thenReturn(0);
        when(mockResponse.getFailedShards()).thenReturn(0);
        when(mockResponse.getShardFailures()).thenReturn(new ShardSearchFailure[0]);
        when(mockResponse.getClusters()).thenReturn(new SearchResponse.Clusters(0, 0, 0));
        when(mockResponse.getAggregations()).thenReturn(null);
        when(mockResponse.getSuggest()).thenReturn(null);
        when(mockResponse.getProfileResults()).thenReturn(null);

        // Call the method under test
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        SearchResponseSectionsProtoUtils.toProto(builder, mockResponse);
        org.opensearch.protobufs.SearchResponse protoResponse = builder.build();

        // Verify processor result - null status should not be set
        assertEquals("Should have 1 processor result", 1, protoResponse.getProcessorResultsCount());
        org.opensearch.protobufs.ProcessorExecutionDetail protoDetail = protoResponse.getProcessorResults(0);
        assertEquals("Processor name should match", "processor1", protoDetail.getProcessorName());
        assertEquals("Duration should match", 100L, protoDetail.getDurationMillis());
        assertFalse("Should not have status", protoDetail.hasStatus());
    }

    public void testToProtoThrowsUnsupportedOperationExceptionForAggregations() {
        // Create mock SearchResponse with aggregations
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));
        when(mockResponse.getAggregations()).thenReturn(mock(org.opensearch.search.aggregations.Aggregations.class));

        // Call the method under test - should throw UnsupportedOperationException
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SearchResponseSectionsProtoUtils.toProto(builder, mockResponse)
        );
        assertEquals("aggregation responses are not supported yet", exception.getMessage());
    }

    public void testToProtoThrowsUnsupportedOperationExceptionForSuggest() {
        // Create mock SearchResponse with suggest
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));
        when(mockResponse.getAggregations()).thenReturn(null);
        when(mockResponse.getSuggest()).thenReturn(mock(org.opensearch.search.suggest.Suggest.class));

        // Call the method under test - should throw UnsupportedOperationException
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SearchResponseSectionsProtoUtils.toProto(builder, mockResponse)
        );
        assertEquals("suggest responses are not supported yet", exception.getMessage());
    }

    public void testToProtoThrowsUnsupportedOperationExceptionForProfileResults() {
        // Create mock SearchResponse with profile results
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mock(SearchResponseSections.class));
        when(mockResponse.getAggregations()).thenReturn(null);
        when(mockResponse.getSuggest()).thenReturn(null);
        Map<String, org.opensearch.search.profile.ProfileShardResult> profileResults = new HashMap<>();
        profileResults.put("shard1", mock(org.opensearch.search.profile.ProfileShardResult.class));
        when(mockResponse.getProfileResults()).thenReturn(profileResults);

        // Call the method under test - should throw UnsupportedOperationException
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SearchResponseSectionsProtoUtils.toProto(builder, mockResponse)
        );
        assertEquals("profile results are not supported yet", exception.getMessage());
    }

    public void testToProtoThrowsUnsupportedOperationExceptionForSearchExtBuilders() {
        // Create mock SearchResponseSections with search ext builders
        SearchResponseSections mockSections = mock(SearchResponseSections.class);
        List<org.opensearch.search.SearchExtBuilder> extBuilders = new ArrayList<>();
        extBuilders.add(mock(org.opensearch.search.SearchExtBuilder.class));
        when(mockSections.getSearchExtBuilders()).thenReturn(extBuilders);

        // Create mock SearchResponse
        SearchResponse mockResponse = mock(SearchResponse.class);
        when(mockResponse.getHits()).thenReturn(SearchHits.empty());
        when(mockResponse.getInternalResponse()).thenReturn(mockSections);
        when(mockResponse.getAggregations()).thenReturn(null);
        when(mockResponse.getSuggest()).thenReturn(null);
        when(mockResponse.getProfileResults()).thenReturn(null);

        // Call the method under test - should throw UnsupportedOperationException
        org.opensearch.protobufs.SearchResponse.Builder builder = org.opensearch.protobufs.SearchResponse.newBuilder();
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SearchResponseSectionsProtoUtils.toProto(builder, mockResponse)
        );
        assertEquals("ext builder responses are not supported yet", exception.getMessage());
    }
}
