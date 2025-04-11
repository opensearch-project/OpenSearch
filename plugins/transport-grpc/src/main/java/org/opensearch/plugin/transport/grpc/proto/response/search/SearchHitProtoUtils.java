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
        return toInnerProto(hit);
    }

    /**
     * Converts a SearchHit to its Protocol Buffer representation.
     * Similar to {@link SearchHit#toInnerXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param hit The SearchHit to convert
     * @return A Protocol Buffer Hit representation
     * @throws IOException if there's an error during conversion
     */
    protected static org.opensearch.protobufs.Hit toInnerProto(SearchHit hit) throws IOException {
        org.opensearch.protobufs.Hit.Builder hitBuilder = org.opensearch.protobufs.Hit.newBuilder();

        // For inner_hit hits shard is null and that is ok, because the parent search hit has all this information.
        // Even if this was included in the inner_hit hits this would be the same, so better leave it out.
        if (hit.getExplanation() != null && hit.getShard() != null) {
            hitBuilder.setShard(String.valueOf(hit.getShard().getShardId().id()));
            hitBuilder.setNode(hit.getShard().getNodeIdText().string());
        }

        if (hit.getIndex() != null) {
            hitBuilder.setIndex(RemoteClusterAware.buildRemoteIndexName(hit.getClusterAlias(), hit.getIndex()));
        }

        if (hit.getId() != null) {
            hitBuilder.setId(hit.getId());
        }

        if (hit.getNestedIdentity() != null) {
            hitBuilder.setNested(NestedIdentityProtoUtils.toProto(hit.getNestedIdentity()));
        }

        if (hit.getVersion() != -1) {
            hitBuilder.setVersion(hit.getVersion());
        }

        if (hit.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            hitBuilder.setSeqNo(hit.getSeqNo());
            hitBuilder.setPrimaryTerm(hit.getPrimaryTerm());
        }

        if (Float.isNaN(hit.getScore())) {
            hitBuilder.setScore(org.opensearch.protobufs.Hit.Score.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build());
        } else {
            hitBuilder.setScore(org.opensearch.protobufs.Hit.Score.newBuilder().setFloatValue(hit.getScore()).build());
        }

        ObjectMap.Builder objectMapBuilder = ObjectMap.newBuilder();
        for (DocumentField field : hit.getMetaFields().values()) {
            // ignore empty metadata fields
            if (field.getValues().isEmpty()) {
                continue;
            }

            objectMapBuilder.putFields(field.getName(), ObjectMapProtoUtils.toProto(field.getValues()));
        }
        hitBuilder.setMetaFields(objectMapBuilder.build());

        if (hit.getSourceRef() != null) {
            hitBuilder.setSource(ByteString.copyFrom(BytesReference.toBytes(hit.getSourceRef())));
        }
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
        if (hit.getHighlightFields() != null && !hit.getHighlightFields().isEmpty()) {
            for (HighlightField field : hit.getHighlightFields().values()) {
                hitBuilder.putHighlight(field.getName(), HighlightFieldProtoUtils.toProto(field.getFragments()));
            }
        }
        SearchSortValuesProtoUtils.toProto(hitBuilder, hit.getSortValues());
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
        if (hit.getExplanation() != null) {
            hitBuilder.setExplanation(buildExplanation(hit.getExplanation()));
        }
        if (hit.getInnerHits() != null) {
            for (Map.Entry<String, SearchHits> entry : hit.getInnerHits().entrySet()) {
                hitBuilder.putInnerHits(
                    entry.getKey(),
                    InnerHitsResult.newBuilder().setHits(SearchHitsProtoUtils.toProto(entry.getValue())).build()
                );
            }
        }
        return hitBuilder.build();
    }

    /**
     * Recursively builds a Protocol Buffer Explanation from a Lucene Explanation.
     * This method converts the Lucene explanation structure, including nested details,
     * into the corresponding Protocol Buffer representation.
     *
     * @param explanation The Lucene Explanation to convert
     * @return A Protocol Buffer Explanation representation
     * @throws IOException if there's an error during conversion
     */
    private static org.opensearch.protobufs.Explanation buildExplanation(org.apache.lucene.search.Explanation explanation)
        throws IOException {
        org.opensearch.protobufs.Explanation.Builder protoExplanationBuilder = org.opensearch.protobufs.Explanation.newBuilder();
        protoExplanationBuilder.setValue(explanation.getValue().doubleValue());
        protoExplanationBuilder.setDescription(explanation.getDescription());

        org.apache.lucene.search.Explanation[] innerExps = explanation.getDetails();
        if (innerExps != null) {
            for (Explanation exp : innerExps) {
                protoExplanationBuilder.addDetails(buildExplanation(exp));
            }
        }
        return protoExplanationBuilder.build();
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
         *
         * @param nestedIdentity The NestedIdentity to convert
         * @return A Protocol Buffer NestedIdentity representation
         */
        protected static NestedIdentity toProto(SearchHit.NestedIdentity nestedIdentity) {
            return innerToProto(nestedIdentity);
        }

        /**
         * Converts a SearchHit.NestedIdentity to its Protocol Buffer representation.
         * Similar to {@link SearchHit.NestedIdentity#innerToXContent(XContentBuilder, ToXContent.Params)}
         *
         * @param nestedIdentity The NestedIdentity to convert
         * @return A Protocol Buffer NestedIdentity representation
         */
        protected static NestedIdentity innerToProto(SearchHit.NestedIdentity nestedIdentity) {
            NestedIdentity.Builder nestedIdentityBuilder = NestedIdentity.newBuilder();
            if (nestedIdentity.getField() != null) {
                nestedIdentityBuilder.setField(nestedIdentity.getField().string());
            }
            if (nestedIdentity.getOffset() != -1) {
                nestedIdentityBuilder.setOffset(nestedIdentity.getOffset());
            }
            if (nestedIdentity.getChild() != null) {
                nestedIdentityBuilder.setNested(toProto(nestedIdentity.getChild()));
            }

            return nestedIdentityBuilder.build();
        }
    }
}
