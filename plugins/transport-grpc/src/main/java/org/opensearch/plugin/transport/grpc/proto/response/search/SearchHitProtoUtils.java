/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.search;

import com.google.protobuf.ByteString;
import org.apache.lucene.search.Explanation;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.plugin.transport.grpc.proto.response.common.ObjectMapProtoUtils;
import org.opensearch.protobufs.InnerHitsResult;
import org.opensearch.protobufs.NestedIdentity;
import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.fetch.subphase.highlight.HighlightField;
import org.opensearch.transport.RemoteClusterAware;

import java.io.IOException;
import java.util.Map;

/**
 * Utility class for converting SearchHit objects to Protocol Buffers.
 * This class handles the conversion of search hit data to their
 * Protocol Buffer representation for gRPC communication.
 */
public class SearchHitProtoUtils {

    private SearchHitProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a SearchHit to its Protocol Buffer representation.
     * This method is equivalent to {@link SearchHit#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param hit The SearchHit to convert
     * @return A Protocol Buffer Hit representation
     * @throws IOException if there's an error during conversion
     */
    protected static org.opensearch.protobufs.Hit toProto(SearchHit hit) throws IOException {
        org.opensearch.protobufs.Hit.Builder hitBuilder = org.opensearch.protobufs.Hit.newBuilder();
        toProto(hit, hitBuilder);
        return hitBuilder.build();
    }

    /**
     * Converts a SearchHit to its Protocol Buffer representation.
     * This method is equivalent to {@link SearchHit#toInnerXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param hit The SearchHit to convert
     * @param hitBuilder The builder to populate with the SearchHit data
     * @throws IOException if there's an error during conversion
     */
    protected static void toProto(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) throws IOException {
        // Process shard information
        processShardInfo(hit, hitBuilder);

        // Process basic hit information
        processBasicInfo(hit, hitBuilder);

        // Process score
        processScore(hit, hitBuilder);

        // Process metadata fields
        processMetadataFields(hit, hitBuilder);

        // Process source
        processSource(hit, hitBuilder);

        // Process document fields
        processDocumentFields(hit, hitBuilder);

        // Process highlight fields
        processHighlightFields(hit, hitBuilder);

        // Process sort values
        SearchSortValuesProtoUtils.toProto(hitBuilder, hit.getSortValues());

        // Process matched queries
        processMatchedQueries(hit, hitBuilder);

        // Process explanation
        processExplanation(hit, hitBuilder);

        // Process inner hits
        processInnerHits(hit, hitBuilder);
    }

    /**
     * Helper method to process shard information.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the shard information
     */
    private static void processShardInfo(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) {
        // For inner_hit hits shard is null and that is ok, because the parent search hit has all this information.
        // Even if this was included in the inner_hit hits this would be the same, so better leave it out.
        if (hit.getExplanation() != null && hit.getShard() != null) {
            hitBuilder.setShard(String.valueOf(hit.getShard().getShardId().id()));
            hitBuilder.setNode(hit.getShard().getNodeIdText().string());
        }
    }

    /**
     * Helper method to process basic hit information.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the basic information
     */
    private static void processBasicInfo(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) {
        // Set index if available
        if (hit.getIndex() != null) {
            hitBuilder.setIndex(RemoteClusterAware.buildRemoteIndexName(hit.getClusterAlias(), hit.getIndex()));
        }

        // Set ID if available
        if (hit.getId() != null) {
            hitBuilder.setId(hit.getId());
        }

        // Set nested identity if available
        if (hit.getNestedIdentity() != null) {
            hitBuilder.setNested(NestedIdentityProtoUtils.toProto(hit.getNestedIdentity()));
        }

        // Set version if available
        if (hit.getVersion() != -1) {
            hitBuilder.setVersion(hit.getVersion());
        }

        // Set sequence number and primary term if available
        if (hit.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            hitBuilder.setSeqNo(hit.getSeqNo());
            hitBuilder.setPrimaryTerm(hit.getPrimaryTerm());
        }
    }

    /**
     * Helper method to process score information.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the score information
     */
    private static void processScore(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) {
        org.opensearch.protobufs.Hit.Score.Builder scoreBuilder = org.opensearch.protobufs.Hit.Score.newBuilder();

        if (Float.isNaN(hit.getScore())) {
            scoreBuilder.setNullValue(NullValue.NULL_VALUE_NULL);
        } else {
            scoreBuilder.setFloatValue(hit.getScore());
        }

        hitBuilder.setScore(scoreBuilder.build());
    }

    /**
     * Helper method to process metadata fields.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the metadata fields
     */
    private static void processMetadataFields(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) {
        // Only process if there are non-empty metadata fields
        if (hit.getMetaFields().values().stream().anyMatch(field -> !field.getValues().isEmpty())) {
            ObjectMap.Builder objectMapBuilder = ObjectMap.newBuilder();

            for (DocumentField field : hit.getMetaFields().values()) {
                // ignore empty metadata fields
                if (field.getValues().isEmpty()) {
                    continue;
                }

                objectMapBuilder.putFields(field.getName(), ObjectMapProtoUtils.toProto(field.getValues()));
            }

            hitBuilder.setMetaFields(objectMapBuilder.build());
        }
    }

    /**
     * Helper method to process source information.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the source information
     */
    private static void processSource(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) {
        if (hit.getSourceRef() != null) {
            hitBuilder.setSource(ByteString.copyFrom(BytesReference.toBytes(hit.getSourceRef())));
        }
    }

    /**
     * Helper method to process document fields.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the document fields
     */
    private static void processDocumentFields(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) {
        if (!hit.getDocumentFields().isEmpty() &&
        // ignore fields all together if they are all empty
            hit.getDocumentFields().values().stream().anyMatch(df -> !df.getValues().isEmpty())) {

            ObjectMap.Builder fieldsStructBuilder = ObjectMap.newBuilder();

            for (DocumentField field : hit.getDocumentFields().values()) {
                if (!field.getValues().isEmpty()) {
                    fieldsStructBuilder.putFields(field.getName(), ObjectMapProtoUtils.toProto(field.getValues()));
                }
            }

            hitBuilder.setFields(fieldsStructBuilder.build());
        }
    }

    /**
     * Helper method to process highlight fields.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the highlight fields
     */
    private static void processHighlightFields(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) {
        if (hit.getHighlightFields() != null && !hit.getHighlightFields().isEmpty()) {
            for (HighlightField field : hit.getHighlightFields().values()) {
                hitBuilder.putHighlight(field.getName(), HighlightFieldProtoUtils.toProto(field.getFragments()));
            }
        }
    }

    /**
     * Helper method to process matched queries.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the matched queries
     */
    private static void processMatchedQueries(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) {
        if (hit.getMatchedQueries().length > 0) {
            // TODO pass params in
            // boolean includeMatchedQueriesScore = params.paramAsBoolean(RestSearchAction.INCLUDE_NAMED_QUERIES_SCORE_PARAM, false);
            boolean includeMatchedQueriesScore = false;

            if (includeMatchedQueriesScore) {
                // TODO map type is missing in spec
                // for (Map.Entry<String, Float> entry : matchedQueries.entrySet()) {
                // hitBuilder.putMatchedqueires(entry.getKey(), entry.getValue());
            } else {
                for (String matchedFilter : hit.getMatchedQueries()) {
                    hitBuilder.addMatchedQueries(matchedFilter);
                }
            }
        }
    }

    /**
     * Helper method to process explanation.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the explanation
     * @throws IOException if there's an error during conversion
     */
    private static void processExplanation(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) throws IOException {
        if (hit.getExplanation() != null) {
            org.opensearch.protobufs.Explanation.Builder explanationBuilder = org.opensearch.protobufs.Explanation.newBuilder();
            buildExplanation(hit.getExplanation(), explanationBuilder);
            hitBuilder.setExplanation(explanationBuilder.build());
        }
    }

    /**
     * Helper method to process inner hits.
     *
     * @param hit The SearchHit to process
     * @param hitBuilder The builder to populate with the inner hits
     * @throws IOException if there's an error during conversion
     */
    private static void processInnerHits(SearchHit hit, org.opensearch.protobufs.Hit.Builder hitBuilder) throws IOException {
        if (hit.getInnerHits() != null) {
            for (Map.Entry<String, SearchHits> entry : hit.getInnerHits().entrySet()) {
                org.opensearch.protobufs.HitsMetadata.Builder hitsBuilder = org.opensearch.protobufs.HitsMetadata.newBuilder();
                SearchHitsProtoUtils.toProto(entry.getValue(), hitsBuilder);

                hitBuilder.putInnerHits(entry.getKey(), InnerHitsResult.newBuilder().setHits(hitsBuilder.build()).build());
            }
        }
    }

    /**
     * Recursively builds a Protocol Buffer Explanation from a Lucene Explanation.
     * This method converts the Lucene explanation structure, including nested details,
     * into the corresponding Protocol Buffer representation.
     *
     * @param explanation The Lucene Explanation to convert
     * @param protoExplanationBuilder The builder to populate with the explanation data
     * @throws IOException if there's an error during conversion
     */
    private static void buildExplanation(
        org.apache.lucene.search.Explanation explanation,
        org.opensearch.protobufs.Explanation.Builder protoExplanationBuilder
    ) throws IOException {
        protoExplanationBuilder.setValue(explanation.getValue().doubleValue());
        protoExplanationBuilder.setDescription(explanation.getDescription());

        org.apache.lucene.search.Explanation[] innerExps = explanation.getDetails();
        if (innerExps != null) {
            for (Explanation exp : innerExps) {
                org.opensearch.protobufs.Explanation.Builder detailBuilder = org.opensearch.protobufs.Explanation.newBuilder();
                buildExplanation(exp, detailBuilder);
                protoExplanationBuilder.addDetails(detailBuilder.build());
            }
        }
    }

    /**
     * Utility class for converting NestedIdentity components between OpenSearch and Protocol Buffers formats.
     * This class handles the transformation of nested document identity information to ensure proper
     * representation of nested search hits.
     */
    protected static class NestedIdentityProtoUtils {
        /**
         * Private constructor to prevent instantiation.
         * This is a utility class with only static methods.
         */
        private NestedIdentityProtoUtils() {
            // Utility class, no instances
        }

        /**
         * Converts a SearchHit.NestedIdentity to its Protocol Buffer representation.
         * Similar to {@link SearchHit.NestedIdentity#toXContent(XContentBuilder, ToXContent.Params)}
         * This method creates a new builder and returns a built value.
         *
         * @param nestedIdentity The NestedIdentity to convert
         * @return A Protocol Buffer NestedIdentity representation
         */
        protected static NestedIdentity toProto(SearchHit.NestedIdentity nestedIdentity) {
            NestedIdentity.Builder nestedIdentityBuilder = NestedIdentity.newBuilder();
            toProto(nestedIdentity, nestedIdentityBuilder);
            return nestedIdentityBuilder.build();
        }

        /**
         * Converts a SearchHit.NestedIdentity to its Protocol Buffer representation.
         * Similar to {@link SearchHit.NestedIdentity#innerToXContent(XContentBuilder, ToXContent.Params)}
         *
         * @param nestedIdentity The NestedIdentity to convert
         * @param nestedIdentityBuilder The builder to populate with the nested identity data
         */
        protected static void toProto(SearchHit.NestedIdentity nestedIdentity, NestedIdentity.Builder nestedIdentityBuilder) {
            // Set field if available
            if (nestedIdentity.getField() != null) {
                nestedIdentityBuilder.setField(nestedIdentity.getField().string());
            }

            // Set offset if available
            if (nestedIdentity.getOffset() != -1) {
                nestedIdentityBuilder.setOffset(nestedIdentity.getOffset());
            }

            // Set child if available
            if (nestedIdentity.getChild() != null) {
                NestedIdentity.Builder childBuilder = NestedIdentity.newBuilder();
                toProto(nestedIdentity.getChild(), childBuilder);
                nestedIdentityBuilder.setNested(childBuilder.build());
            }
        }
    }
}
