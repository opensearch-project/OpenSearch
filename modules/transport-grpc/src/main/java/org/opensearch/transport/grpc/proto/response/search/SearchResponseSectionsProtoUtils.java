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
import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Utility class for converting SearchResponse objects to Protocol Buffers.
 * This class handles the conversion of search operation responses to their
 * Protocol Buffer representation.
 * <p>
 * This is the gRPC transport equivalent of the REST API's XContent serialization.
 * The REST side uses {@link SearchResponseSections#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params)}
 * to serialize search responses to JSON, while this class converts to protobuf format.
 * <p>
 * Key conversions:
 * <ul>
 *   <li>Hits: {@link org.opensearch.search.SearchHits} → HitsMetadata proto</li>
 *   <li>Aggregations: {@link org.opensearch.search.aggregations.InternalAggregations} → Map&lt;String, Aggregate&gt; proto</li>
 *   <li>Processor results: {@link org.opensearch.search.pipeline.ProcessorExecutionDetail} → ProcessorExecutionDetail proto</li>
 * </ul>
 *
 * @see org.opensearch.action.search.SearchResponseSections
 * @see org.opensearch.action.search.SearchResponse
 */
public class SearchResponseSectionsProtoUtils {

    private SearchResponseSectionsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a SearchResponse to its Protocol Buffer representation.
     * Similar to {@link SearchResponseSections#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param builder The Protocol Buffer SearchResponse builder to populate
     * @param response The SearchResponse to convert
     * @throws IOException if there's an error during conversion
     */
    protected static void toProto(org.opensearch.protobufs.SearchResponse.Builder builder, SearchResponse response) throws IOException {
        // Convert hits using pass by reference
        org.opensearch.protobufs.HitsMetadata.Builder hitsBuilder = org.opensearch.protobufs.HitsMetadata.newBuilder();
        SearchHitsProtoUtils.toProto(response.getHits(), hitsBuilder);
        builder.setHits(hitsBuilder.build());

        // Convert aggregations if present
        // Similar to REST API serialization in SearchResponseSections.toXContent()
        // REST side: aggregations.toXContent(builder, params) - serializes to JSON
        // Proto side: We convert InternalAggregations to a map of Aggregate protobufs
        // @see org.opensearch.action.search.SearchResponseSections#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params)
        // @see org.opensearch.search.aggregations.Aggregations#toXContent(org.opensearch.core.xcontent.XContentBuilder, org.opensearch.core.xcontent.ToXContent.Params)
        if (response.getAggregations() != null) {
            InternalAggregations aggregations = (InternalAggregations) response.getAggregations();
            Map<String, Aggregate> aggregatesMap = new HashMap<>();

            for (org.opensearch.search.aggregations.Aggregation agg : aggregations.asList()) {
                try {
                    aggregatesMap.put(agg.getName(), AggregateProtoUtils.toProto((InternalAggregation) agg));
                } catch (IllegalArgumentException e) {
                    // Aggregation type not yet supported - throw UnsupportedOperationException
                    throw new UnsupportedOperationException("Aggregation type not supported: " + agg.getClass().getName(), e);
                }
            }

            builder.putAllAggregations(aggregatesMap);
        }

        // Convert processor results
        List<org.opensearch.search.pipeline.ProcessorExecutionDetail> processorResults = response.getInternalResponse()
            .getProcessorResult();
        if (processorResults != null && !processorResults.isEmpty()) {
            for (org.opensearch.search.pipeline.ProcessorExecutionDetail detail : processorResults) {
                org.opensearch.protobufs.ProcessorExecutionDetail.Builder detailBuilder = org.opensearch.protobufs.ProcessorExecutionDetail
                    .newBuilder();
                if (detail.getProcessorName() != null) {
                    detailBuilder.setProcessorName(detail.getProcessorName());
                }
                detailBuilder.setDurationMillis(detail.getDurationMillis());
                if (detail.getInputData() != null) {
                    detailBuilder.setInputData(ObjectMapProtoUtils.toProto(detail.getInputData()).getObjectMap());
                }
                if (detail.getOutputData() != null) {
                    detailBuilder.setOutputData(ObjectMapProtoUtils.toProto(detail.getOutputData()).getObjectMap());
                }
                if (detail.getStatus() != null) {
                    detailBuilder.setStatus(detail.getStatus().name().toLowerCase(Locale.ROOT));
                }
                if (detail.getErrorMessage() != null) {
                    detailBuilder.setError(detail.getErrorMessage());
                }
                if (detail.getTag() != null) {
                    detailBuilder.setTag(detail.getTag());
                }
                builder.addProcessorResults(detailBuilder.build());
            }
        }

        // Check for unsupported features
        checkUnsupportedFeatures(response);
    }

    /**
     * Helper method to check for unsupported features.
     *
     * @param response The SearchResponse to check
     * @throws UnsupportedOperationException if unsupported features are present
     */
    private static void checkUnsupportedFeatures(SearchResponse response) {
        // TODO: Implement suggest conversion
        if (response.getSuggest() != null) {
            throw new UnsupportedOperationException("suggest responses are not supported yet");
        }

        // TODO: Implement profile results conversion
        if (response.getProfileResults() != null && !response.getProfileResults().isEmpty()) {
            throw new UnsupportedOperationException("profile results are not supported yet");
        }

        // TODO: Implement search ext builders conversion
        if (response.getInternalResponse().getSearchExtBuilders() != null
            && !response.getInternalResponse().getSearchExtBuilders().isEmpty()) {
            throw new UnsupportedOperationException("ext builder responses are not supported yet");
        }
    }
}
