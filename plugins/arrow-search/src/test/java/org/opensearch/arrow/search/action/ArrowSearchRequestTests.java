/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.arrow.search.Column;
import org.opensearch.arrow.search.ColumnResolver;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.search.Scroll;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.collapse.CollapseBuilder;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.slice.SliceBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ArrowSearchRequestTests extends OpenSearchTestCase {

    private static final ArrowBatchSink SINK = new ArrowBatchSink() {
        @Override
        public void sendSchema(Schema schema) {}

        @Override
        public void sendBatch(VectorSchemaRoot batch) {
            batch.close();
        }

        @Override
        public void complete(SearchResponse summary) {}

        @Override
        public void fail(Exception failure) {}

        @Override
        public void reset() {}

        @Override
        public void setOnCancel(Runnable onCancel) {}
    };

    private static ArrowSearchRequest wrap(SearchRequest request) {
        return new ArrowSearchRequest(request, SINK, TimeValue.MINUS_ONE);
    }

    private static ArrowSearchRequest request(Consumer<SearchSourceBuilder> source) {
        SearchSourceBuilder builder = new SearchSourceBuilder().docValueField("a").docValueField("b*");
        source.accept(builder);
        return wrap(new SearchRequest("idx").source(builder));
    }

    private static void assertRejected(String option, ArrowSearchRequest request) {
        ActionRequestValidationException e = request.validate();
        assertNotNull(option, e);
        assertTrue(e.getMessage(), e.getMessage().contains("[" + option + "] is not supported by Arrow search"));
    }

    public void testValidFilterRequest() {
        ArrowSearchRequest request = request(
            s -> s.query(QueryBuilders.termQuery("k", "v"))
                .postFilter(QueryBuilders.existsQuery("a"))
                .timeout(TimeValue.timeValueSeconds(5))
                .size(10)
                .terminateAfter(3)
        );
        assertNull(request.validate());
        assertEquals(
            List.of(new ColumnResolver.RequestedField("a", false), new ColumnResolver.RequestedField("b*", false)),
            request.requestedFields()
        );
        assertEquals(10, request.rowLimit());
        assertEquals(3, request.shardRowLimit());
    }

    public void testLimitsDefaultToUnlimited() {
        ArrowSearchRequest request = request(s -> {});
        assertEquals(Long.MAX_VALUE, request.rowLimit());
        assertEquals(Long.MAX_VALUE, request.shardRowLimit());
        assertEquals(5, request(s -> s.size(5).terminateAfter(50)).shardRowLimit());
    }

    public void testRequiresDocValueFields() {
        ArrowSearchRequest noSource = wrap(new SearchRequest("idx"));
        assertTrue(noSource.validate().getMessage().contains("[docvalue_fields] must name the fields"));
        ArrowSearchRequest noFields = wrap(new SearchRequest("idx").source(new SearchSourceBuilder()));
        assertTrue(noFields.validate().getMessage().contains("[docvalue_fields] must name the fields"));
    }

    public void testRejectsDocValueFormatsOtherThanList() {
        ActionRequestValidationException e = request(s -> s.docValueField("c", "epoch_millis")).validate();
        assertTrue(e.getMessage(), e.getMessage().contains("format [epoch_millis] is not supported"));
    }

    public void testListFormatRequestsAListColumn() {
        ArrowSearchRequest request = request(s -> s.docValueField("tags", Column.LIST_FORMAT));
        assertNull(request.validate());
        assertEquals(new ColumnResolver.RequestedField("tags", true), request.requestedFields().get(2));
    }

    public void testRejectsOptionsNeedingScoresOrderOrFetch() {
        assertRejected("aggregations", request(s -> s.aggregation(AggregationBuilders.terms("t").field("a"))));
        assertRejected("suggest", request(s -> s.suggest(new SuggestBuilder())));
        assertRejected("highlight", request(s -> s.highlighter(new HighlightBuilder())));
        assertRejected("collapse", request(s -> s.collapse(new CollapseBuilder("a"))));
        assertRejected("search_after", request(s -> s.searchAfter(new Object[] { 1 })));
        assertRejected("from", request(s -> s.from(5)));
        assertRejected("sort", request(s -> s.sort("a", SortOrder.ASC)));
        assertRejected("slice", request(s -> s.slice(new SliceBuilder(0, 2))));
        assertRejected("min_score", request(s -> s.minScore(1f)));
        assertRejected("track_scores", request(s -> s.trackScores(true)));
        assertRejected("explain", request(s -> s.explain(true)));
        assertRejected("profile", request(s -> s.profile(true)));
        assertRejected("version", request(s -> s.version(true)));
        assertRejected("seq_no_primary_term", request(s -> s.seqNoAndPrimaryTerm(true)));
        assertRejected("script_fields", request(s -> s.scriptField("x", new Script("1"))));
        assertRejected("fields", request(s -> s.fetchField("a")));
        assertRejected("stored_fields", request(s -> s.storedField("a")));
        assertRejected("indices_boost", request(s -> s.indexBoost("idx", 2f)));
        assertRejected("pit", request(s -> s.pointInTimeBuilder(new PointInTimeBuilder("id"))));
        assertRejected("ext", request(s -> s.ext(List.of(new TestExt()))));
    }

    public void testExplicitFalseFlagsAreAccepted() {
        assertNull(request(s -> s.explain(false).version(false).seqNoAndPrimaryTerm(false)).validate());
    }

    public void testRejectsSourceFiltering() {
        assertRejected("_source", request(s -> s.fetchSource(true)));
        assertRejected("_source", request(s -> s.fetchSource("b", null)));
        assertRejected("_source", request(s -> s.fetchSource(null, "b")));
        assertNull("_source: false asks for nothing", request(s -> s.fetchSource(false)).validate());
    }

    public void testRejectsInnerHits() {
        assertRejected("inner_hits", request(s -> s.query(nestedQuery().innerHit(new InnerHitBuilder()))));
        assertRejected(
            "inner_hits",
            request(s -> s.postFilter(QueryBuilders.boolQuery().filter(nestedQuery().innerHit(new InnerHitBuilder()))))
        );
        assertRejected(
            "inner_hits",
            request(
                s -> s.query(
                    QueryBuilders.boolQuery()
                        .filter(nestedQuery().innerHit(new InnerHitBuilder("n")))
                        .filter(nestedQuery().innerHit(new InnerHitBuilder("n")))
                )
            )
        );
        assertNull(request(s -> s.query(nestedQuery()).postFilter(nestedQuery())).validate());
    }

    private static NestedQueryBuilder nestedQuery() {
        return QueryBuilders.nestedQuery("n", QueryBuilders.matchAllQuery(), ScoreMode.None);
    }

    public void testRejectsRequestLevelOptions() {
        SearchSourceBuilder source = new SearchSourceBuilder().docValueField("a");
        assertRejected("scroll", wrap(new SearchRequest("idx").source(source).scroll(new Scroll(TimeValue.timeValueMinutes(1)))));
        assertRejected("search_type", wrap(new SearchRequest("idx").source(source).searchType(SearchType.DFS_QUERY_THEN_FETCH)));
        assertRejected("search_pipeline", wrap(new SearchRequest("idx").source(source).pipeline("p")));
    }

    public void testIsASearchRequestOfTheOriginalIndices() {
        SearchRequest original = new SearchRequest("x", "y").routing("r")
            .preference("_local")
            .source(new SearchSourceBuilder().docValueField("a"));
        ArrowSearchRequest request = wrap(original);
        assertArrayEquals(new String[] { "x", "y" }, request.indices());
        assertEquals("r", request.routing());
        assertEquals("_local", request.preference());
        assertEquals(original.indicesOptions(), request.indicesOptions());
    }

    public void testTaskIsCancellableAndUntracked() {
        ArrowSearchTask task = request(s -> {}).createTask(1, "transport", ArrowSearchAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());
        assertTrue(task.shouldCancelChildrenOnCancellation());
        assertFalse("an export runs as long as its client reads", task.supportsResourceTracking());
        assertTrue(task.getDescription(), task.getDescription().startsWith("arrow indices[idx]"));
        assertTrue(task.getDescription(), task.getDescription().contains("\"docvalue_fields\":[{\"field\":\"a\"},{\"field\":\"b*\"}]"));
    }

    public void testCancelAfterIntervalOfTheRequestWins() {
        SearchRequest search = new SearchRequest("idx").source(new SearchSourceBuilder().docValueField("a"));
        ArrowSearchTask byDefault = new ArrowSearchRequest(search, SINK, TimeValue.timeValueMinutes(10)).createTask(
            1,
            "transport",
            ArrowSearchAction.NAME,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        assertEquals(TimeValue.timeValueMinutes(10), byDefault.getCancellationTimeout());

        search.setCancelAfterTimeInterval(TimeValue.timeValueSeconds(30));
        ArrowSearchTask requested = new ArrowSearchRequest(search, SINK, TimeValue.timeValueMinutes(10)).createTask(
            2,
            "transport",
            ArrowSearchAction.NAME,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        assertEquals(TimeValue.timeValueSeconds(30), requested.getCancellationTimeout());
    }

    public void testTaskCancellationRunsTheCallbackOnce() {
        ArrowSearchTask task = request(s -> {}).createTask(1, "transport", ArrowSearchAction.NAME, TaskId.EMPTY_TASK_ID, Map.of());
        AtomicInteger calls = new AtomicInteger();
        task.setOnCancel(calls::incrementAndGet);
        task.cancel("test");
        assertEquals(1, calls.get());
        AtomicInteger late = new AtomicInteger();
        task.setOnCancel(late::incrementAndGet);
        assertEquals("a callback set after cancellation runs at once", 1, late.get());
    }

    /** A search extension a client could send. */
    private static final class TestExt extends SearchExtBuilder {
        @Override
        public String getWriteableName() {
            return "test";
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) {
            return builder;
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TestExt;
        }
    }

    public void testIsNeverSerialized() {
        expectThrows(UnsupportedOperationException.class, () -> request(s -> {}).writeTo(new BytesStreamOutput()));
    }
}
