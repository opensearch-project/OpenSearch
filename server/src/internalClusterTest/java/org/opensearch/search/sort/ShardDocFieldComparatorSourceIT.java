/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort;

import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitAction;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 2, supportsDedicatedMasters = false)
public class ShardDocFieldComparatorSourceIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "test_shard_doc";

    @Before
    public void setupIndex() {
        createIndex(INDEX, Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0).build());
        ensureGreen(INDEX);
    }

    public void testEmptyIndex() throws Exception {
        String pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
        try {
            SearchSourceBuilder ssb = new SearchSourceBuilder().size(10)
                .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
                .pointInTimeBuilder(pit(pitId));
            SearchResponse resp = client().search(new SearchRequest(INDEX).source(ssb)).actionGet();

            // no hits at all
            SearchHit[] hits = resp.getHits().getHits();
            assertThat(hits.length, equalTo(0));
            assertThat(resp.getHits().getTotalHits().value(), equalTo(0L));
        } finally {
            closePit(pitId);
        }
    }

    public void testSingleDocument() throws Exception {
        client().prepareIndex(INDEX).setId("42").setSource("foo", "bar").get();
        refresh();

        String pitId = null;
        try {
            pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
            SearchSourceBuilder ssb = new SearchSourceBuilder().size(5)
                .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
                .pointInTimeBuilder(pit(pitId));
            SearchResponse resp = client().search(new SearchRequest(INDEX).source(ssb)).actionGet();

            assertThat(resp.getHits().getTotalHits().value(), equalTo(1L));
            assertThat(resp.getHits().getHits()[0].getId(), equalTo("42"));
        } finally {
            closePit(pitId);
        }
    }

    public void testSearchAfterBeyondEndYieldsNoHits() throws Exception {
        indexSequentialDocs(5);
        refresh();
        List<Long> allKeys = new ArrayList<>();

        String pitId = null;
        try {
            pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
            SearchSourceBuilder ssb = new SearchSourceBuilder().size(5)
                .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
                .pointInTimeBuilder(pit(pitId));

            SearchResponse resp0 = client().search(new SearchRequest(INDEX).source(ssb)).actionGet();
            // collect first page
            for (SearchHit hit : resp0.getHits().getHits()) {
                Object[] sv = hit.getSortValues();
                allKeys.add(((Number) sv[0]).longValue());
            }

            long globalMax = allKeys.get(allKeys.size() - 1);

            SearchSourceBuilder next = new SearchSourceBuilder().size(3)
                .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
                .pointInTimeBuilder(pit(pitId))
                .searchAfter(new Object[] { globalMax + 1 });

            SearchResponse resp = client().search(new SearchRequest(INDEX).source(next)).actionGet();
            SearchHit[] hits = resp.getHits().getHits();
            assertThat(hits.length, equalTo(0));

        } finally {
            closePit(pitId);
        }
    }

    public void testSearchAfterBeyondEndYieldsNoHits_DESC() throws Exception {
        indexSequentialDocs(5);
        refresh();

        String pitId = null;
        try {
            // First page: _shard_doc DESC, grab the SMALLEST key (last hit on the page)
            pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
            SearchSourceBuilder ssb = new SearchSourceBuilder().size(5)
                .sort(SortBuilders.shardDocSort().order(SortOrder.DESC))
                .pointInTimeBuilder(pit(pitId));

            SearchResponse first = client().search(new SearchRequest(INDEX).source(ssb)).actionGet();
            assertThat(first.getHits().getHits().length, equalTo(5));

            // Probe strictly beyond the end for DESC: use search_after < min (min - 1) => expect 0 hits
            long minKey = ((Number) first.getHits().getHits()[4].getSortValues()[0]).longValue(); // smallest in DESC page
            SearchSourceBuilder probe = new SearchSourceBuilder().size(3)
                .sort(SortBuilders.shardDocSort().order(SortOrder.DESC))
                .pointInTimeBuilder(pit(pitId))
                .searchAfter(new Object[] { minKey - 1 });

            SearchResponse resp = client().search(new SearchRequest(INDEX).source(probe)).actionGet();
            assertThat(resp.getHits().getHits().length, equalTo(0));

        } finally {
            closePit(pitId);
        }
    }

    public void testPrimaryFieldSortThenShardDocTieBreaker() throws Exception {
        // force ties on primary
        for (int i = 1; i <= 30; i++) {
            client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("val", 123).get();
        }
        refresh();

        List<Long> shardDocKeys = new ArrayList<>();
        String pitId = null;
        try {
            pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
            collectIdsAndSortKeys(
                INDEX,
                pitId,
                10,
                1,
                null,
                shardDocKeys,
                new FieldSortBuilder("val").order(SortOrder.ASC),
                SortBuilders.shardDocSort().order(SortOrder.ASC)
            );

            assertThat(shardDocKeys.size(), equalTo(30));
            for (int i = 1; i < shardDocKeys.size(); i++) {
                assertThat(shardDocKeys.get(i), greaterThan(shardDocKeys.get(i - 1)));
            }
        } finally {
            closePit(pitId);
        }
    }

    public void testOrderingAscAndPagination() throws Exception {
        assertShardDocOrdering(SortOrder.ASC);
    }

    public void testOrderingDescAndPagination() throws Exception {
        assertShardDocOrdering(SortOrder.DESC);
    }

    private void assertShardDocOrdering(SortOrder order) throws Exception {
        int pageSize = randomIntBetween(5, 23);
        int totalDocs = randomIntBetween(73, 187);
        indexSequentialDocs(totalDocs);
        refresh();

        String pitId = null;
        try {
            pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
            List<Long> shardDocKeys = new ArrayList<>();
            // shardDocIndex = 0 because we're only sorting by _shard_doc here
            collectIdsAndSortKeys(INDEX, pitId, pageSize, 0, null, shardDocKeys, SortBuilders.shardDocSort().order(order));

            assertThat(shardDocKeys.size(), equalTo(totalDocs));

            for (int i = 1; i < shardDocKeys.size(); i++) {
                if (order == SortOrder.ASC) {
                    assertThat("not strictly increasing at i=" + i, shardDocKeys.get(i), greaterThan(shardDocKeys.get(i - 1)));
                } else {
                    assertThat("not strictly decreasing at i=" + i, shardDocKeys.get(i), lessThan(shardDocKeys.get(i - 1)));
                }
            }

        } finally {
            closePit(pitId);
        }
    }

    public void testPageLocalMonotonicity_ASC() throws Exception {
        indexSequentialDocs(20);
        refresh();

        String pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(10)
            .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
            .pointInTimeBuilder(pit(pitId));

        SearchResponse resp = client().search(new SearchRequest(INDEX).source(ssb)).actionGet();
        SearchHit[] hits = resp.getHits().getHits();
        for (int i = 1; i < hits.length; i++) {
            long prev = ((Number) hits[i - 1].getSortValues()[0]).longValue();
            long cur = ((Number) hits[i].getSortValues()[0]).longValue();
            assertThat("regression at i=" + i, cur, greaterThan(prev));
        }
        closePit(pitId);
    }

    // No duplicates across the whole scan (ASC & DESC).
    public void testNoDuplicatesAcrossScan_ASC_DESC() throws Exception {
        indexSequentialDocs(123);
        refresh();

        String pitId = null;
        try {
            pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
            List<String> idsAsc = new ArrayList<>();
            List<Long> shardDocKeys = new ArrayList<>();

            // ASC
            collectIdsAndSortKeys(INDEX, pitId, 13, 0, idsAsc, shardDocKeys, SortBuilders.shardDocSort().order(SortOrder.ASC));
            assertThat(idsAsc.size(), equalTo(123));
            assertThat(new HashSet<>(idsAsc).size(), equalTo(idsAsc.size()));

            // DESC
            List<String> idsDesc = new ArrayList<>();
            collectIdsAndSortKeys(INDEX, pitId, 17, 0, idsDesc, shardDocKeys, SortBuilders.shardDocSort().order(SortOrder.DESC));
            assertThat(idsDesc.size(), equalTo(123));
            assertThat(new HashSet<>(idsDesc).size(), equalTo(idsDesc.size()));
        } finally {
            closePit(pitId);
        }
    }

    // Resume from the middle of a page (ASC).
    public void testResumeFromMiddleOfPage_ASC() throws Exception {
        indexSequentialDocs(60);
        refresh();

        String pitId = null;
        try {
            pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));

            // First page to pick a middle anchor
            SearchSourceBuilder firstPage = new SearchSourceBuilder().size(10)
                .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
                .pointInTimeBuilder(pit(pitId));
            SearchResponse r1 = client().search(new SearchRequest(INDEX).source(firstPage)).actionGet();
            assertThat(r1.getHits().getHits().length, equalTo(10));

            int mid = 4;
            Object[] midSort = r1.getHits().getHits()[mid].getSortValues();

            // Collect IDs = first page up to 'mid' (inclusive), then resume from mid sort tuple
            List<String> ids = new ArrayList<>();
            for (int i = 0; i <= mid; i++) {
                ids.add(r1.getHits().getHits()[i].getId());
            }

            SearchSourceBuilder resume = new SearchSourceBuilder().size(10)
                .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
                .pointInTimeBuilder(pit(pitId))
                .searchAfter(midSort);
            SearchResponse resp = client().search(new SearchRequest(INDEX).source(resume)).actionGet();

            while (true) {
                SearchHit[] hits = resp.getHits().getHits();
                // should start strictly after the anchor
                for (SearchHit h : hits)
                    ids.add(h.getId());
                if (hits.length < 10) break;
                Object[] after = hits[hits.length - 1].getSortValues();

                resume = new SearchSourceBuilder().size(10)
                    .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
                    .pointInTimeBuilder(pit(pitId))
                    .searchAfter(after);
                resp = client().search(new SearchRequest(INDEX).source(resume)).actionGet();
            }

            // Should cover all 60 docs exactly once
            assertThat(ids.size(), equalTo(60));
            assertThat(new HashSet<>(ids).size(), equalTo(60));
        } finally {
            closePit(pitId);
        }
    }

    // Tiny page sizes (size=1 and size=2) with strict monotonicity & no dupes.
    public void testTinyPageSizes_ASC() throws Exception {
        indexSequentialDocs(41);
        refresh();

        for (int pageSize : new int[] { 1, 2 }) {
            String pitId = null;
            try {
                pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
                List<Long> keys = new ArrayList<>();
                collectIdsAndSortKeys(INDEX, pitId, pageSize, 0, null, keys, SortBuilders.shardDocSort().order(SortOrder.ASC));

                assertThat(keys.size(), equalTo(41));
                for (int i = 1; i < keys.size(); i++) {
                    assertThat(keys.get(i), greaterThan(keys.get(i - 1)));
                }
            } finally {
                closePit(pitId);
            }
        }
    }

    // Replicas enabled: still strict order and no dupes.
    public void testWithReplicasEnabled_ASC() throws Exception {
        final String repIdx = INDEX + "_repl";
        createIndex(repIdx, Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 1).build());
        ensureGreen(repIdx);

        for (int i = 1; i <= 100; i++) {
            client().prepareIndex(repIdx).setId(Integer.toString(i)).setSource("v", i).get();
        }
        refresh(repIdx);

        String pitId = null;
        try {
            pitId = openPit(repIdx, TimeValue.timeValueMinutes(1));
            List<Long> keys = new ArrayList<>();
            List<String> ids = new ArrayList<>();
            collectIdsAndSortKeys(repIdx, pitId, 11, 0, ids, keys, SortBuilders.shardDocSort().order(SortOrder.ASC));
            assertThat(keys.size(), equalTo(100));
            for (int i = 1; i < keys.size(); i++) {
                assertThat(keys.get(i), greaterThan(keys.get(i - 1)));
            }
            // also IDs unique
            // List<String> ids = collectAllIds(repIdx, pitId, 11, SortBuilders.shardDocSort().order(SortOrder.ASC));
            assertThat(new HashSet<>(ids).size(), equalTo(ids.size()));
        } finally {
            closePit(pitId);
        }
    }

    // Boundary equality: using the exact last sort tuple as search_after should not duplicate the boundary doc.
    public void testBoundaryEqualityNoOverlap_ASC() throws Exception {
        indexSequentialDocs(30);
        refresh();

        String pitId = null;
        try {
            pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));

            SearchSourceBuilder p1 = new SearchSourceBuilder().size(7)
                .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
                .pointInTimeBuilder(pit(pitId));
            SearchResponse r1 = client().search(new SearchRequest(INDEX).source(p1)).actionGet();
            SearchHit[] hits1 = r1.getHits().getHits();
            SearchHit lastOfPage1 = hits1[hits1.length - 1];

            SearchSourceBuilder p2 = new SearchSourceBuilder().size(7)
                .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
                .pointInTimeBuilder(pit(pitId))
                .searchAfter(lastOfPage1.getSortValues());
            SearchResponse r2 = client().search(new SearchRequest(INDEX).source(p2)).actionGet();
            SearchHit[] hits2 = r2.getHits().getHits();

            if (hits2.length > 0) {
                assertNotEquals("no overlap with boundary", lastOfPage1.getId(), hits2[0].getId());
            }
        } finally {
            closePit(pitId);
        }
    }

    // Large corpus, odd page sizes, multi-shard interleaving stress.
    public void testLargeCorpusInterleaving_ASC() throws Exception {
        final String bigIdx = INDEX + "_big";
        createIndex(bigIdx, Settings.builder().put("index.number_of_shards", 5).put("index.number_of_replicas", 1).build());
        ensureGreen(TimeValue.timeValueSeconds(60), bigIdx);

        for (int i = 1; i <= 2000; i++) {
            client().prepareIndex(bigIdx).setId(Integer.toString(i)).setSource("v", i).get();
        }
        refresh(bigIdx);

        String pitId = null;
        try {
            pitId = openPit(bigIdx, TimeValue.timeValueMinutes(1));
            // odd page sizes to stress boundaries
            int[] sizes = new int[] { 13, 17, 19, 23, 31 };
            for (int sz : sizes) {
                List<Long> keys = new ArrayList<>();
                // shardDocIndex=0 since only shard_doc is sorted
                collectIdsAndSortKeys(INDEX, pitId, sz, 0, null, keys, SortBuilders.shardDocSort().order(SortOrder.ASC));
                assertThat(keys.size(), equalTo(2000));
                for (int i = 1; i < keys.size(); i++) {
                    assertThat(keys.get(i), greaterThan(keys.get(i - 1)));
                }
            }
        } finally {
            closePit(pitId);
        }
    }

    private void indexSequentialDocs(int count) {
        for (int i = 1; i <= count; i++) {
            client().prepareIndex(INDEX)
                .setId(Integer.toString(i))
                // the content doesn't matter for _shard_doc
                .setSource("val", i)
                .get();
        }
    }

    private String openPit(String index, TimeValue keepAlive) throws Exception {
        CreatePitRequest request = new CreatePitRequest(keepAlive, true);
        request.setIndices(new String[] { index });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        return pitResponse.getId();
    }

    private void closePit(String pitId) {
        if (pitId == null) return;
        DeletePitRequest del = new DeletePitRequest(Collections.singletonList(pitId));
        client().execute(DeletePitAction.INSTANCE, del).actionGet();
    }

    private static PointInTimeBuilder pit(String pitId) {
        return new PointInTimeBuilder(pitId).setKeepAlive(TimeValue.timeValueMinutes(1));
    }

    // Generic paginator: works for 1 or many sort keys.
    // - pageSize: page size
    // - shardDocIndex: which position in sortValues[]
    // - sorts: the full sort list to apply (e.g., only _shard_doc, or primary then _shard_doc)
    private void collectIdsAndSortKeys(
        String index,
        String pitId,
        int pageSize,
        int shardDocIndex,
        List<String> ids,
        List<Long> keys,
        SortBuilder<?>... sorts
    ) {
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(pageSize).pointInTimeBuilder(pit(pitId));
        for (var s : sorts) {
            ssb.sort(s);
        }
        SearchResponse resp = client().search(new SearchRequest(index).source(ssb)).actionGet();

        while (true) {
            SearchHit[] hits = resp.getHits().getHits();
            for (SearchHit hit : hits) {
                Object[] sv = hit.getSortValues();
                assertNotNull("every hit must have sort", sv);
                assertTrue("shard_doc should be present", shardDocIndex < sv.length);
                assertThat("sort key must be a Long", sv[shardDocIndex], instanceOf(Long.class));
                long k = (Long) sv[shardDocIndex];
                keys.add(k);
                if (ids != null) {
                    ids.add(hit.getId());
                }
            }
            // stop if last page
            if (hits.length < pageSize) break;

            // use the FULL last sortValues[] as search_after for correctness
            Object[] nextAfter = hits[hits.length - 1].getSortValues();
            ssb = new SearchSourceBuilder().size(pageSize).pointInTimeBuilder(pit(pitId));
            for (var s : sorts) {
                ssb.sort(s);
            }
            ssb.searchAfter(nextAfter);

            resp = client().search(new SearchRequest(index).source(ssb)).actionGet();
        }
    }
}
