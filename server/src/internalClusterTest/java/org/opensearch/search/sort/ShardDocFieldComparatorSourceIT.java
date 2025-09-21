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
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(10)
            .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
            .pointInTimeBuilder(pit(pitId));
        SearchResponse resp = client().search(new SearchRequest(INDEX).source(ssb)).actionGet();

        // no hits at all
        SearchHit[] hits = resp.getHits().getHits();
        assertThat(hits.length, equalTo(0));
        assertThat(resp.getHits().getTotalHits().value(), equalTo(0L));
        closePit(pitId);
    }

    public void testSingleDocument() throws Exception {
        client().prepareIndex(INDEX).setId("42").setSource("foo", "bar").get();
        refresh();

        String pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
        SearchSourceBuilder ssb = new SearchSourceBuilder().size(5)
            .sort(SortBuilders.shardDocSort().order(SortOrder.ASC))
            .pointInTimeBuilder(pit(pitId));
        SearchResponse resp = client().search(new SearchRequest(INDEX).source(ssb)).actionGet();

        assertThat(resp.getHits().getTotalHits().value(), equalTo(1L));
        assertThat(resp.getHits().getHits()[0].getId(), equalTo("42"));
        closePit(pitId);
    }

    public void testSearchAfterBeyondEndYieldsNoHits() throws Exception {
        indexSequentialDocs(5);
        refresh();
        List<Long> allKeys = new ArrayList<>();

        String pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
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
        closePit(pitId);
    }

    public void testSearchAfterBeyondEndYieldsNoHits_DESC() throws Exception {
        indexSequentialDocs(5);
        refresh();

        // First page: _shard_doc DESC, grab the SMALLEST key (last hit on the page)
        String pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
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
        closePit(pitId);
    }

    public void testPrimaryFieldSortThenShardDocTieBreaker() throws Exception {
        // force ties on primary
        for (int i = 1; i <= 30; i++) {
            client().prepareIndex(INDEX).setId(Integer.toString(i)).setSource("val", 123).get();
        }
        refresh();

        String pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
        var shardDocKeys = collectAllSortKeys(
            pitId,
            10,
            1,
            new FieldSortBuilder("val").order(SortOrder.ASC),
            SortBuilders.shardDocSort().order(SortOrder.ASC)
        );

        assertThat(shardDocKeys.size(), equalTo(30));
        for (int i = 1; i < shardDocKeys.size(); i++) {
            assertThat(shardDocKeys.get(i), greaterThan(shardDocKeys.get(i - 1)));
        }
        closePit(pitId);
    }

    public void testOrderingAscAndPagination() throws Exception {
        assertShardDocOrdering(SortOrder.ASC, 7, 20);
    }

    public void testOrderingDescAndPagination() throws Exception {
        assertShardDocOrdering(SortOrder.DESC, 8, 20);
    }

    private void assertShardDocOrdering(SortOrder order, int pageSize, int expectedCount) throws Exception {
        indexSequentialDocs(expectedCount);
        refresh();

        String pitId = openPit(INDEX, TimeValue.timeValueMinutes(1));
        // shardDocIndex = 0 because we're only sorting by _shard_doc here
        List<Long> keys = collectAllSortKeys(pitId, pageSize, 0, SortBuilders.shardDocSort().order(order));

        assertThat(keys.size(), equalTo(expectedCount));

        for (int i = 1; i < keys.size(); i++) {
            if (order == SortOrder.ASC) {
                assertThat("not strictly increasing at i=" + i, keys.get(i), greaterThan(keys.get(i - 1)));
            } else {
                assertThat("not strictly decreasing at i=" + i, keys.get(i), lessThan(keys.get(i - 1)));
            }
        }
        closePit(pitId);
    }

    // Generic paginator: works for 1 or many sort keys.
    // - pageSize: page size
    // - shardDocIndex: which position in sortValues[]
    // - sorts: the full sort list to apply (e.g., only _shard_doc, or primary then _shard_doc)
    private List<Long> collectAllSortKeys(String pitId, int pageSize, int shardDocIndex, SortBuilder<?>... sorts) {
        List<Long> all = new ArrayList<>();

        SearchSourceBuilder ssb = new SearchSourceBuilder().size(pageSize).pointInTimeBuilder(pit(pitId));
        for (var s : sorts) {
            ssb.sort(s);
        }
        SearchResponse resp = client().search(new SearchRequest(INDEX).source(ssb)).actionGet();

        while (true) {
            for (SearchHit hit : resp.getHits().getHits()) {
                Object[] sv = hit.getSortValues();
                all.add(((Number) sv[shardDocIndex]).longValue());
            }
            // stop if last page
            if (resp.getHits().getHits().length < pageSize) break;

            // use the FULL last sortValues[] as search_after for correctness
            Object[] nextAfter = resp.getHits().getHits()[resp.getHits().getHits().length - 1].getSortValues();

            ssb = new SearchSourceBuilder().size(pageSize).pointInTimeBuilder(pit(pitId));
            for (var s : sorts) {
                ssb.sort(s);
            }
            ssb.searchAfter(nextAfter);

            resp = client().search(new SearchRequest(INDEX).source(ssb)).actionGet();
        }
        return all;
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
}
