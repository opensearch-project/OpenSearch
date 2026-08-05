/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common.helpers;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchShardInfo;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SearchResponseUtilTests extends OpenSearchTestCase {

    public void testReplaceHitsPreservesPhaseTookAndShardInfo() {
        SearchShardInfo shardInfo = new SearchShardInfo(
            List.of(new SearchShardInfo.Entry.Builder("idx", 0).nodeId("node-1").primary(true).state("STARTED").build()),
            Collections.emptyList(),
            Collections.emptyList()
        );
        SearchResponse.PhaseTook phaseTook = new SearchResponse.PhaseTook(Map.of("query", 10L));
        SearchHits originalHits = new SearchHits(new SearchHit[] { new SearchHit(0) }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse response = new SearchResponse(
            new InternalSearchResponse(originalHits, null, null, null, false, null, 1),
            null,
            1,
            1,
            0,
            42,
            phaseTook,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY,
            null,
            shardInfo
        );

        SearchResponse replaced = SearchResponseUtil.replaceHits(new SearchHit[] { new SearchHit(1) }, response);

        assertEquals(1, replaced.getHits().getHits().length);
        // a hit-rewriting response processor must not silently strip the opt-in response sections
        assertEquals(phaseTook, replaced.getPhaseTook());
        assertEquals(shardInfo, replaced.getShardInfo());
        assertEquals(response.getTotalShards(), replaced.getTotalShards());
        assertEquals(response.getSuccessfulShards(), replaced.getSuccessfulShards());
        assertEquals(response.getSkippedShards(), replaced.getSkippedShards());
    }
}
