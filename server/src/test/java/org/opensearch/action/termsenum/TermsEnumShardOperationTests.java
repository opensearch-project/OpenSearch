/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.action.OriginalIndices;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.SearchService;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;

public class TermsEnumShardOperationTests extends OpenSearchSingleNodeTestCase {

    /** Hides every field whose name starts with {@code secret}, the way field-level security does. */
    public static class HideSecretFieldsPlugin extends Plugin implements MapperPlugin {
        @Override
        public Function<String, Predicate<String>> getFieldFilter() {
            return index -> field -> field.startsWith("secret") == false;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(HideSecretFieldsPlugin.class);
    }

    private static Settings oneShard() {
        return Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0).build();
    }

    private void createMapped(String index, String mapping) {
        createIndexWithMappingSource(index, oneShard(), mapping);
    }

    private void createMapped(String index, Settings extra, String mapping) {
        createIndexWithMappingSource(index, Settings.builder().put(oneShard()).put(extra).build(), mapping);
    }

    private void index(String index, String id, String json) {
        client().prepareIndex(index).setId(id).setSource(json, MediaTypeRegistry.JSON).get();
    }

    private TermsEnumShardResponse run(String index, TermsEnumRequest r) throws IOException {
        return run(index, r, System::nanoTime, System.nanoTime());
    }

    private TermsEnumShardResponse run(String index, TermsEnumRequest r, LongSupplier clock, long startNanos) throws IOException {
        return run(index, r, clock, startNanos, newTask());
    }

    private TermsEnumShardResponse run(String index, TermsEnumRequest r, LongSupplier clock, long startNanos, SearchShardTask task)
        throws IOException {
        // the transport handler waits for a pending refresh before executing; refresh here so execute sees every doc
        client().admin().indices().prepareRefresh(index).get();
        TermsEnumShardRequest shardRequest = new TermsEnumShardRequest(
            new ShardId(resolveIndex(index), 0),
            new OriginalIndices(new String[] { index }, IndicesOptions.strictExpandOpen()),
            r,
            System.currentTimeMillis(),
            60_000
        );
        TermsEnumShardOperation operation = new TermsEnumShardOperation(
            getInstanceFromNode(IndicesService.class),
            getInstanceFromNode(SearchService.class),
            clock
        );
        return operation.execute(shardRequest, task, startNanos);
    }

    private static SearchShardTask newTask() {
        return new SearchShardTask(1, "transport", TransportTermsEnumAction.SHARD_ACTION_NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
    }

    private static void assertOk(TermsEnumShardResponse response, List<String> terms) {
        assertEquals(TermsEnumShardResponse.Outcome.OK, response.outcome());
        assertEquals(terms, response.terms());
        assertFalse(response.timedOut());
    }

    public void testKeywordTerms() throws IOException {
        createMapped("idx", "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}");
        index("idx", "1", "{\"tag\":\"web-02\"}");
        index("idx", "2", "{\"tag\":\"web-01\"}");
        index("idx", "3", "{\"tag\":\"api-01\"}");
        index("idx", "4", "{\"tag\":\"web-01\"}");
        assertOk(run("idx", new TermsEnumRequest("idx").field("tag").string("web")), List.of("web-01", "web-02"));
    }

    public void testTermOnlyInDeletedDocIsNotReturned() throws IOException {
        createMapped("idx", "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}");
        index("idx", "1", "{\"tag\":\"ghost\"}");
        index("idx", "2", "{\"tag\":\"live\"}");
        client().admin().indices().prepareRefresh("idx").get();
        client().prepareDelete("idx", "1").get();
        client().admin().indices().prepareRefresh("idx").get();
        assertOk(run("idx", new TermsEnumRequest("idx").field("tag")), List.of("live"));
    }

    public void testConstantKeywordOnEmptyIndex() throws IOException {
        createMapped("idx", "{\"properties\":{\"team\":{\"type\":\"constant_keyword\",\"value\":\"red\"}}}");
        assertOk(run("idx", new TermsEnumRequest("idx").field("team")), List.of());
    }

    public void testConstantKeywordWithDocs() throws IOException {
        createMapped("idx", "{\"properties\":{\"team\":{\"type\":\"constant_keyword\",\"value\":\"red\"}}}");
        index("idx", "1", "{\"other\":1}");
        assertOk(run("idx", new TermsEnumRequest("idx").field("team")), List.of("red"));
    }

    public void testIndexMetaField() throws IOException {
        createMapped("idx", "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}");
        index("idx", "1", "{\"tag\":\"a\"}");
        client().admin().indices().prepareRefresh("idx").get();
        assertOk(run("idx", new TermsEnumRequest("idx").field("_index").string("")), List.of("idx"));
    }

    public void testFieldHiddenByFieldFilterIsAbsent() throws IOException {
        createMapped(
            "idx",
            "{\"properties\":{\"secret_tag\":{\"type\":\"keyword\"},"
                + "\"secret_team\":{\"type\":\"constant_keyword\",\"value\":\"red\"}}}"
        );
        index("idx", "1", "{\"secret_tag\":\"hidden\"}");
        assertEquals(TermsEnumShardResponse.fieldAbsent(), run("idx", new TermsEnumRequest("idx").field("secret_tag")));
        assertEquals(TermsEnumShardResponse.fieldAbsent(), run("idx", new TermsEnumRequest("idx").field("secret_team")));
    }

    public void testUnmappedFieldIsAbsent() throws IOException {
        createMapped("idx", "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}");
        index("idx", "1", "{\"tag\":\"a\"}");
        assertEquals(TermsEnumShardResponse.fieldAbsent(), run("idx", new TermsEnumRequest("idx").field("nope")));
    }

    public void testUnsupportedFieldTypes() throws IOException {
        createMapped(
            "idx",
            "{\"properties\":{"
                + "\"num\":{\"type\":\"long\"},"
                + "\"txt\":{\"type\":\"text\"},"
                + "\"wc\":{\"type\":\"wildcard\"},"
                + "\"flat\":{\"type\":\"flat_object\"},"
                + "\"unindexed\":{\"type\":\"keyword\",\"index\":false}"
                + "}}"
        );
        index("idx", "1", "{\"num\":1,\"txt\":\"a b\",\"wc\":\"abc\",\"flat\":{\"a\":\"x\"},\"unindexed\":\"u\"}");
        for (String field : List.of("num", "txt", "wc", "flat", "flat.a", "flat._value", "unindexed")) {
            TermsEnumShardResponse response = run("idx", new TermsEnumRequest("idx").field(field));
            assertEquals(field, TermsEnumShardResponse.Outcome.FIELD_UNSUPPORTED, response.outcome());
            assertThat(field, response.reason(), containsString("does not support terms enumeration"));
        }
        // a mapping-defined derived field is unsupported whether or not derived fields are enabled on the index
        String derived = "{\"derived\":{\"d\":{\"type\":\"keyword\",\"script\":{\"source\":\"s\",\"lang\":\"mockscript\"}}},"
            + "\"properties\":{\"tag\":{\"type\":\"keyword\"}}}";
        createMapped("derived-enabled", derived);
        createMapped("derived-disabled", Settings.builder().put("index.query.derived_field.enabled", false).build(), derived);
        for (String index : List.of("derived-enabled", "derived-disabled")) {
            index(index, "1", "{\"tag\":\"a\"}");
            TermsEnumShardResponse response = run(index, new TermsEnumRequest(index).field("d"));
            assertEquals(index, TermsEnumShardResponse.Outcome.FIELD_UNSUPPORTED, response.outcome());
            assertThat(index, response.reason(), containsString("of type [derived] does not support terms enumeration"));
        }
    }

    public void testAliasAndMultiFieldResolveToConcreteField() throws IOException {
        createMapped(
            "idx",
            "{\"properties\":{"
                + "\"tag\":{\"type\":\"keyword\"},"
                + "\"tag_alias\":{\"type\":\"alias\",\"path\":\"tag\"},"
                + "\"secret_tag\":{\"type\":\"keyword\"},"
                + "\"public_alias\":{\"type\":\"alias\",\"path\":\"secret_tag\"}"
                + "}}"
        );
        index("idx", "1", "{\"tag\":\"t1\",\"secret_tag\":\"s1\",\"host\":\"h1\"}");
        index("idx", "2", "{\"tag\":\"t2\",\"host\":\"h2\"}");
        assertOk(run("idx", new TermsEnumRequest("idx").field("tag_alias")), List.of("t1", "t2"));
        assertOk(run("idx", new TermsEnumRequest("idx").field("host.keyword")), List.of("h1", "h2"));
        // the filter tests the concrete field name, so an alias cannot expose a hidden field
        assertEquals(TermsEnumShardResponse.fieldAbsent(), run("idx", new TermsEnumRequest("idx").field("public_alias")));
    }

    public void testIndexFilterSkipsShard() throws IOException {
        createMapped("idx", "{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"tag\":{\"type\":\"keyword\"}}}");
        index("idx", "1", "{\"@timestamp\":\"2026-05-01\",\"tag\":\"a\"}");
        index("idx", "2", "{\"@timestamp\":\"2026-06-01\",\"tag\":\"b\"}");
        TermsEnumRequest future = new TermsEnumRequest("idx").field("tag")
            .indexFilter(QueryBuilders.rangeQuery("@timestamp").gte("2030-01-01"));
        assertEquals(TermsEnumShardResponse.skipped(), run("idx", future));
        TermsEnumRequest past = new TermsEnumRequest("idx").field("tag")
            .indexFilter(QueryBuilders.rangeQuery("@timestamp").gte("2000-01-01"));
        assertOk(run("idx", past), List.of("a", "b"));
    }

    public void testDeadlineReturnsPartialTerms() throws IOException {
        createMapped("idx", "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}");
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < 300; i++) {
            bulk.add(client().prepareIndex("idx").setId(Integer.toString(i)).setSource("tag", term(i)));
        }
        assertFalse(bulk.get().hasFailures());
        // the clock reads far past the 60 s budget, so the first deadline check stops the collection
        TermsEnumShardResponse response = run("idx", new TermsEnumRequest("idx").field("tag").size(300), () -> Long.MAX_VALUE / 2, 0);
        assertEquals(TermsEnumShardResponse.Outcome.OK, response.outcome());
        assertTrue(response.timedOut());
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            expected.add(term(i));
        }
        assertEquals(expected, response.terms());
    }

    private static String term(int i) {
        return String.format(Locale.ROOT, "t%03d", i);
    }

    public void testCancelledTaskThrows() throws IOException {
        createMapped("idx", "{\"properties\":{\"tag\":{\"type\":\"keyword\"}}}");
        index("idx", "1", "{\"tag\":\"a\"}");
        SearchShardTask task = newTask();
        task.cancel("test");
        expectThrows(
            TaskCancelledException.class,
            () -> run("idx", new TermsEnumRequest("idx").field("tag"), System::nanoTime, System.nanoTime(), task)
        );
    }
}
