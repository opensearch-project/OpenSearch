/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.response.search;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.protobufs.HitXScore;
import org.opensearch.protobufs.HitsMetadata;
import org.opensearch.protobufs.HitsMetadataHitsInner;
import org.opensearch.protobufs.HitsMetadataMaxScore;
import org.opensearch.protobufs.HitsMetadataTotal;
import org.opensearch.protobufs.ShardStatistics;
import org.opensearch.protobufs.TotalHitsRelation;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;

import java.util.Map;

/**
 * Converts a {@code protobufs.SearchResponse} (received over gRPC) into the server-internal {@link SearchResponse}.
 *
 * <p>This is the inverse of {@link SearchResponseProtoUtils}, which lives next door. Co-locating both directions
 * enables a round-trip parity test that catches divergence as the schema evolves.
 *
 * <p><b>Known lossy or deferred conversions</b>:
 * <ul>
 *   <li>Aggregations / suggest / profile / processor results are not decoded — server-side encoders for these are stubs
 *       on the proto path today, so they should not appear in responses.</li>
 *   <li>Shard failures are not yet decoded — empty array placeholder.</li>
 *   <li>Per-hit shard target uses a placeholder {@link ShardId} ({@link Index} uuid {@code "_na_"}, shard id {@code -1})
 *       since proto carries no shard UUID/id.</li>
 *   <li>NaN scores: the forward path emits a {@code null_value} sentinel for {@code NaN}; we restore to {@link Float#NaN}.</li>
 *   <li>Per-hit fields not yet decoded: {@code sortValues}, {@code fields}, {@code highlightFields},
 *       {@code matchedQueries}, {@code explanation}, {@code innerHits}, {@code nestedIdentity}, {@code ignored}.</li>
 * </ul>
 */
public final class SearchResponseProtoConverter {

    private SearchResponseProtoConverter() {}

    /**
     * Converts a {@code protobufs.SearchResponse} into a server-internal {@link SearchResponse}.
     *
     * @param proto the proto response received over gRPC
     * @return the equivalent {@link SearchResponse}
     */
    public static SearchResponse fromProto(org.opensearch.protobufs.SearchResponse proto) {
        SearchHits hits = toSearchHits(proto.hasHits() ? proto.getHits() : null);
        SearchResponseSections sections = new SearchResponseSections(hits, null, null, proto.getTimedOut(), null, null, 0);

        ShardStatistics shards = proto.hasXShards() ? proto.getXShards() : null;
        int totalShards = shards != null ? shards.getTotal() : 0;
        int successful = shards != null ? shards.getSuccessful() : 0;
        int skipped = shards != null && shards.hasSkipped() ? shards.getSkipped() : 0;

        return new SearchResponse(
            sections,
            null,                       // scrollId — deferred
            totalShards,
            successful,
            skipped,
            proto.getTook(),
            new ShardSearchFailure[0],  // shard failures — deferred
            SearchResponse.Clusters.EMPTY
        );
    }

    private static SearchHits toSearchHits(HitsMetadata hm) {
        if (hm == null) {
            return new SearchHits(new SearchHit[0], new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN);
        }
        int n = hm.getHitsCount();
        SearchHit[] arr = new SearchHit[n];
        for (int i = 0; i < n; i++) {
            arr[i] = toSearchHit(hm.getHits(i));
        }
        TotalHits total = toTotalHits(hm.hasTotal() ? hm.getTotal() : null);
        float maxScore = hm.hasMaxScore() ? toFloat(hm.getMaxScore()) : Float.NaN;
        return new SearchHits(arr, total, maxScore);
    }

    private static SearchHit toSearchHit(HitsMetadataHitsInner h) {
        SearchHit hit = new SearchHit(
            -1,                                  // docId placeholder
            h.hasXId() ? h.getXId() : null,
            null,                                // nestedIdentity — deferred
            Map.of(),                            // documentFields — deferred
            Map.of()                             // metaFields — deferred
        );

        if (h.hasXScore()) hit.score(toFloat(h.getXScore()));
        if (h.hasXVersion()) hit.version(h.getXVersion());
        if (h.hasXSeqNo()) hit.setSeqNo(h.getXSeqNo());
        if (h.hasXPrimaryTerm()) hit.setPrimaryTerm(h.getXPrimaryTerm());
        if (h.hasXSource()) hit.sourceRef(new BytesArray(h.getXSource().toByteArray()));

        if (h.hasXIndex()) {
            ShardId sid = new ShardId(new Index(h.getXIndex(), "_na_"), -1);
            hit.shard(new SearchShardTarget(null, sid, null, OriginalIndices.NONE));
        }
        return hit;
    }

    private static TotalHits toTotalHits(HitsMetadataTotal t) {
        if (t == null) {
            return new TotalHits(0, TotalHits.Relation.EQUAL_TO);
        }
        switch (t.getHitsMetadataTotalCase()) {
            case TOTAL_HITS: {
                org.opensearch.protobufs.TotalHits th = t.getTotalHits();
                TotalHits.Relation rel = th.getRelation() == TotalHitsRelation.TOTAL_HITS_RELATION_GTE
                    ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO
                    : TotalHits.Relation.EQUAL_TO;
                return new TotalHits(th.getValue(), rel);
            }
            case INT64:
                return new TotalHits(t.getInt64(), TotalHits.Relation.EQUAL_TO);
            case HITSMETADATATOTAL_NOT_SET:
            default:
                return new TotalHits(0, TotalHits.Relation.EQUAL_TO);
        }
    }

    private static float toFloat(HitXScore s) {
        return s.hasNullValue() ? Float.NaN : (float) s.getDouble();
    }

    private static float toFloat(HitsMetadataMaxScore m) {
        return m.hasNullValue() ? Float.NaN : m.getFloat();
    }
}
