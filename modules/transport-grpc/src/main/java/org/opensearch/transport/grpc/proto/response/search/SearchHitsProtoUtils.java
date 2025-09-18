/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search;

import org.apache.lucene.search.TotalHits;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.NullValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;

import java.io.IOException;

/**
 * Utility class for converting SearchHits objects to Protocol Buffers.
 * This class handles the conversion of search operation responses to their
 * Protocol Buffer representation.
 */
public class SearchHitsProtoUtils {

    private SearchHitsProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a SearchHits to its Protocol Buffer representation.
     * This method is equivalent to {@link SearchHits#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param hits The SearchHits to convert
     * @return A Protocol Buffer HitsMetadata representation
     * @throws IOException if there's an error during conversion
     */
    protected static org.opensearch.protobufs.HitsMetadata toProto(SearchHits hits) throws IOException {
        org.opensearch.protobufs.HitsMetadata.Builder hitsMetaData = org.opensearch.protobufs.HitsMetadata.newBuilder();
        toProto(hits, hitsMetaData);
        return hitsMetaData.build();
    }

    /**
     * Converts a SearchHits to its Protocol Buffer representation.
     * This method is equivalent to {@link SearchHits#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param hits The SearchHits to convert
     * @param hitsMetaData The builder to populate with the SearchHits data
     * @throws IOException if there's an error during conversion
     */
    protected static void toProto(SearchHits hits, org.opensearch.protobufs.HitsMetadata.Builder hitsMetaData) throws IOException {
        // Process total hits information
        processTotalHits(hits, hitsMetaData);

        // Process max score information
        processMaxScore(hits, hitsMetaData);

        // Process individual hits
        processHits(hits, hitsMetaData);
    }

    /**
     * Helper method to process total hits information.
     *
     * @param hits The SearchHits to process
     * @param hitsMetaData The builder to populate with the total hits data
     */
    private static void processTotalHits(SearchHits hits, org.opensearch.protobufs.HitsMetadata.Builder hitsMetaData) {
        org.opensearch.protobufs.HitsMetadataTotal.Builder totalBuilder = org.opensearch.protobufs.HitsMetadataTotal.newBuilder();

        // TODO need to pass parameters
        // boolean totalHitAsInt = params.paramAsBoolean(RestSearchAction.TOTAL_HITS_AS_INT_PARAM, false);
        boolean totalHitAsInt = false;

        if (totalHitAsInt) {
            long total = hits.getTotalHits() == null ? -1 : hits.getTotalHits().value();
            totalBuilder.setInt64(total);
        } else if (hits.getTotalHits() != null) {
            org.opensearch.protobufs.TotalHits.Builder totalHitsBuilder = org.opensearch.protobufs.TotalHits.newBuilder();
            totalHitsBuilder.setValue(hits.getTotalHits().value());

            // Set relation based on the TotalHits relation
            org.opensearch.protobufs.TotalHitsRelation relation = hits.getTotalHits().relation() == TotalHits.Relation.EQUAL_TO
                ? org.opensearch.protobufs.TotalHitsRelation.TOTAL_HITS_RELATION_EQ
                : org.opensearch.protobufs.TotalHitsRelation.TOTAL_HITS_RELATION_GTE;
            totalHitsBuilder.setRelation(relation);

            totalBuilder.setTotalHits(totalHitsBuilder.build());
        }

        hitsMetaData.setTotal(totalBuilder.build());
    }

    /**
     * Helper method to process max score information.
     *
     * @param hits The SearchHits to process
     * @param hitsMetaData The builder to populate with the max score data
     */
    private static void processMaxScore(SearchHits hits, org.opensearch.protobufs.HitsMetadata.Builder hitsMetaData) {
        org.opensearch.protobufs.HitsMetadataMaxScore.Builder maxScoreBuilder = org.opensearch.protobufs.HitsMetadataMaxScore.newBuilder();

        if (Float.isNaN(hits.getMaxScore())) {
            maxScoreBuilder.setNullValue(NullValue.NULL_VALUE_NULL);
        } else {
            maxScoreBuilder.setFloat(hits.getMaxScore());
        }

        hitsMetaData.setMaxScore(maxScoreBuilder.build());
    }

    /**
     * Helper method to process individual hits.
     *
     * @param hits The SearchHits to process
     * @param hitsMetaData The builder to populate with the hits data
     * @throws IOException if there's an error during conversion
     */
    private static void processHits(SearchHits hits, org.opensearch.protobufs.HitsMetadata.Builder hitsMetaData) throws IOException {
        // Process each hit
        for (SearchHit hit : hits) {
            org.opensearch.protobufs.HitsMetadataHitsInner.Builder hitBuilder = org.opensearch.protobufs.HitsMetadataHitsInner.newBuilder();
            SearchHitProtoUtils.toProto(hit, hitBuilder);
            hitsMetaData.addHits(hitBuilder.build());
        }
    }
}
