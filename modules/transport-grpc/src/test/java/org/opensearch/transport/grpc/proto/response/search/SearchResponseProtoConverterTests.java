/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import com.google.protobuf.ByteString;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.protobufs.HitXScore;
import org.opensearch.protobufs.HitsMetadata;
import org.opensearch.protobufs.HitsMetadataHitsInner;
import org.opensearch.protobufs.HitsMetadataMaxScore;
import org.opensearch.protobufs.HitsMetadataTotal;
import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.ShardStatistics;
import org.opensearch.protobufs.TotalHitsRelation;
import org.opensearch.search.SearchHit;
import org.opensearch.test.OpenSearchTestCase;

public class SearchResponseProtoConverterTests extends OpenSearchTestCase {

    public void testTookAndTimedOut_roundTrip() {
        org.opensearch.protobufs.SearchResponse proto = baseProto().setTook(123L).setTimedOut(true).build();
        SearchResponse server = SearchResponseProtoConverter.fromProto(proto);
        assertEquals(123L, server.getTook().millis());
        assertTrue(server.isTimedOut());
    }

    public void testShards_roundTrip() {
        ShardStatistics shards = ShardStatistics.newBuilder().setTotal(5).setSuccessful(4).setSkipped(1).setFailed(0).build();
        org.opensearch.protobufs.SearchResponse proto = baseProto().setXShards(shards).build();
        SearchResponse server = SearchResponseProtoConverter.fromProto(proto);
        assertEquals(5, server.getTotalShards());
        assertEquals(4, server.getSuccessfulShards());
        assertEquals(1, server.getSkippedShards());
    }

    public void testTotalHits_eqRelation() {
        org.opensearch.protobufs.SearchResponse proto = baseProto().setHits(
            HitsMetadata.newBuilder()
                .setTotal(
                    HitsMetadataTotal.newBuilder()
                        .setTotalHits(
                            org.opensearch.protobufs.TotalHits.newBuilder()
                                .setValue(42L)
                                .setRelation(TotalHitsRelation.TOTAL_HITS_RELATION_EQ)
                                .build()
                        )
                        .build()
                )
                .build()
        ).build();
        SearchResponse server = SearchResponseProtoConverter.fromProto(proto);
        TotalHits total = server.getHits().getTotalHits();
        assertEquals(42L, total.value());
        assertEquals(TotalHits.Relation.EQUAL_TO, total.relation());
    }

    public void testTotalHits_gteRelation() {
        org.opensearch.protobufs.SearchResponse proto = baseProto().setHits(
            HitsMetadata.newBuilder()
                .setTotal(
                    HitsMetadataTotal.newBuilder()
                        .setTotalHits(
                            org.opensearch.protobufs.TotalHits.newBuilder()
                                .setValue(10000L)
                                .setRelation(TotalHitsRelation.TOTAL_HITS_RELATION_GTE)
                                .build()
                        )
                        .build()
                )
                .build()
        ).build();
        TotalHits total = SearchResponseProtoConverter.fromProto(proto).getHits().getTotalHits();
        assertEquals(10000L, total.value());
        assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, total.relation());
    }

    public void testNaNScoreSentinel_perHit() {
        HitsMetadataHitsInner hit = HitsMetadataHitsInner.newBuilder()
            .setXIndex("idx")
            .setXId("1")
            .setXScore(HitXScore.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build())
            .build();
        org.opensearch.protobufs.SearchResponse proto = baseProto().setHits(HitsMetadata.newBuilder().addHits(hit).build()).build();
        SearchResponse server = SearchResponseProtoConverter.fromProto(proto);
        assertTrue("score must be NaN, was " + server.getHits().getAt(0).getScore(), Float.isNaN(server.getHits().getAt(0).getScore()));
    }

    public void testNaNScoreSentinel_maxScore() {
        org.opensearch.protobufs.SearchResponse proto = baseProto().setHits(
            HitsMetadata.newBuilder().setMaxScore(HitsMetadataMaxScore.newBuilder().setNullValue(NullValue.NULL_VALUE_NULL).build()).build()
        ).build();
        SearchResponse server = SearchResponseProtoConverter.fromProto(proto);
        assertTrue(Float.isNaN(server.getHits().getMaxScore()));
    }

    public void testHitFields_propagated() {
        HitsMetadataHitsInner hit = HitsMetadataHitsInner.newBuilder()
            .setXIndex("idx")
            .setXId("1")
            .setXVersion(3L)
            .setXSeqNo(7L)
            .setXPrimaryTerm(2L)
            .setXScore(HitXScore.newBuilder().setDouble(1.5d).build())
            .setXSource(ByteString.copyFromUtf8("{\"k\":1}"))
            .build();
        org.opensearch.protobufs.SearchResponse proto = baseProto().setHits(HitsMetadata.newBuilder().addHits(hit).build()).build();

        SearchResponse server = SearchResponseProtoConverter.fromProto(proto);
        SearchHit h = server.getHits().getAt(0);
        assertEquals("idx", h.getIndex());
        assertEquals("1", h.getId());
        assertEquals(3L, h.getVersion());
        assertEquals(7L, h.getSeqNo());
        assertEquals(2L, h.getPrimaryTerm());
        assertEquals(1.5f, h.getScore(), 0.0001f);
        assertEquals("{\"k\":1}", h.getSourceAsString());
    }

    public void testEmptyHits() {
        org.opensearch.protobufs.SearchResponse proto = baseProto().build();
        SearchResponse server = SearchResponseProtoConverter.fromProto(proto);
        assertEquals(0, server.getHits().getHits().length);
    }

    private static org.opensearch.protobufs.SearchResponse.Builder baseProto() {
        return org.opensearch.protobufs.SearchResponse.newBuilder().setTook(1L);
    }
}
