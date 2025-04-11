/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.search;

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

        org.opensearch.protobufs.HitsMetadata.Total.Builder totalBuilder = org.opensearch.protobufs.HitsMetadata.Total.newBuilder();

        // TODO need to pass parameters
        // boolean totalHitAsInt = params.paramAsBoolean(RestSearchAction.TOTAL_HITS_AS_INT_PARAM, false);
        boolean totalHitAsInt = false;
        if (totalHitAsInt) {
            long total = hits.getTotalHits() == null ? -1 : hits.getTotalHits().value();
            totalBuilder.setDoubleValue(total);
        } else if (hits.getTotalHits() != null) {
            org.opensearch.protobufs.TotalHits.Builder totalHitsBuilder = org.opensearch.protobufs.TotalHits.newBuilder();
            totalHitsBuilder.setValue(hits.getTotalHits().value());
            totalHitsBuilder.setRelation(
                hits.getTotalHits().relation() == TotalHits.Relation.EQUAL_TO
                    ? org.opensearch.protobufs.TotalHits.TotalHitsRelation.TOTAL_HITS_RELATION_EQ
                    : org.opensearch.protobufs.TotalHits.TotalHitsRelation.TOTAL_HITS_RELATION_GTE
            );
            totalBuilder.setTotalHits(totalHitsBuilder.build());
        }

        hitsMetaData.setTotal(totalBuilder.build());

        org.opensearch.protobufs.HitsMetadata.MaxScore.Builder maxScoreBuilder = org.opensearch.protobufs.HitsMetadata.MaxScore
            .newBuilder();
        if (Float.isNaN(hits.getMaxScore())) {
            hitsMetaData.setMaxScore(maxScoreBuilder.setNullValue(NullValue.NULL_VALUE_NULL).build()).build();
        } else {
            hitsMetaData.setMaxScore(maxScoreBuilder.setFloatValue(hits.getMaxScore()).build()).build();
        }
        for (SearchHit h : hits) {
            hitsMetaData.addHits(SearchHitProtoUtils.toProto(h));
        }

        return hitsMetaData.build();
    }
}
