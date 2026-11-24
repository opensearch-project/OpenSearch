/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.logging.HeaderWarning;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class IgnoreMappingConflictsIT extends OpenSearchIntegTestCase {
    private static final String SETTING = FieldMapper.IGNORE_MAPPING_CONFLICTS_SETTING.getKey();
    private static final String MAPPING = "{\"properties\":{\"number\":{\"type\":\"long\"},\"good\":{\"type\":\"keyword\"}}}";

    public void testBulkSourceIgnoredSearchAndLiveToggle() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 2).put("index.number_of_replicas", 0))
                .setMapping(MAPPING)
        );
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(SETTING, true)));
        String validRouting = routingKeyForShard("test", 0);
        String conflictRouting = routingKeyForShard("test", 1);
        Client bulkClient = client();
        PlainActionFuture<BulkResponse> result = new PlainActionFuture<>();
        AtomicReference<List<String>> warningHeaders = new AtomicReference<>();
        bulkClient.bulk(
            bulkClient.prepareBulk()
                .add(bulkClient.prepareIndex("test").setId("valid").setRouting(validRouting).setSource("number", 1, "good", "valid"))
                .add(
                    bulkClient.prepareIndex("test").setId("conflict").setRouting(conflictRouting).setSource("number", "bad", "good", "kept")
                )
                .request(),
            ActionListener.wrap(response -> {
                List<String> headers = bulkClient.threadPool().getThreadContext().getResponseHeaders().get("Warning");
                warningHeaders.set(headers == null ? List.of() : List.copyOf(headers));
                result.onResponse(response);
            }, result::onFailure)
        );
        BulkResponse bulk = result.actionGet();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertEquals(
            List.of(
                "Ignored mapping-conflicting fields [number] while parsing document with id [conflict] in index [test]; values preserved in _source"
            ),
            warningHeaders.get()
                .stream()
                .map(header -> HeaderWarning.extractWarningValueFromWarningHeader(header, false))
                .filter(warning -> warning.startsWith("Ignored mapping-conflicting fields"))
                .collect(Collectors.toList())
        );
        for (var item : bulk.getItems()) {
            assertEquals(RestStatus.CREATED, item.status());
        }
        GetResponse get = client().prepareGet("test", "conflict")
            .setRouting(conflictRouting)
            .setStoredFields("_ignored")
            .setFetchSource(true)
            .get();
        assertEquals(Map.of("number", "bad", "good", "kept"), get.getSourceAsMap());
        assertEquals(List.of("number"), get.getField("_ignored").getValues());
        refresh("test");
        SearchResponse response = assertSearchResponse(client().prepareSearch("test").setQuery(termQuery("good", "kept")));
        assertHitCount(response, 1);
        assertEquals(Map.of("number", "bad", "good", "kept"), response.getHits().getAt(0).getSourceAsMap());
        assertHitCount(assertSearchResponse(client().prepareSearch("test").setQuery(termQuery("_ignored", "number"))), 1);
        assertHitCount(assertSearchResponse(client().prepareSearch("test").setQuery(termQuery("number", 1))), 1);
        assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(Settings.builder().put(SETTING, false)));
        expectThrows(MapperParsingException.class, () -> client().prepareIndex("test").setSource("number", "bad").get());
    }

    public void testTotalFieldsLimitStillFailsAfterConflictRecovery() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put(SETTING, true).put("index.mapping.total_fields.limit", 2))
                .setMapping(MAPPING)
        );
        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareIndex("test").setSource("number", "bad", "good", "kept", "extra", 1))
            .get();
        assertTrue(bulk.hasFailures());
        assertThat(bulk.buildFailureMessage(), containsString("Limit of total fields [2]"));
    }

    public void testImmenseTermStillFailsAfterConflictRecovery() throws Exception {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(SETTING, true)).setMapping(MAPPING));
        BulkResponse bulk = client().prepareBulk()
            .add(client().prepareIndex("test").setSource("number", "bad", "good", "x".repeat(32767 + 1)))
            .get();
        assertTrue(bulk.hasFailures());
        assertThat(bulk.buildFailureMessage(), containsString("immense term"));
        // A Lucene rejection records a no-op. Release its buffered translog bytes even with async durability.
        flush("test");
    }

    public void testRejectsIncompleteSourceOnCreationAndSettingsUpdate() throws Exception {
        // The unit tests cover each incompatible source mode; verify the transport paths with one representative.
        String mapping = "{\"_source\":{\"enabled\":false}}";
        MapperParsingException creation = expectThrows(
            MapperParsingException.class,
            () -> prepareCreate("invalid").setSettings(Settings.builder().put(SETTING, true)).setMapping(mapping).get()
        );
        assertThat(creation.getMessage(), containsString("complete stored _source"));
        assertFalse(indexExists("invalid"));
        assertAcked(prepareCreate("strict").setMapping(mapping));
        MapperParsingException update = expectThrows(
            MapperParsingException.class,
            () -> client().admin().indices().prepareUpdateSettings("strict").setSettings(Settings.builder().put(SETTING, true)).get()
        );
        assertThat(update.getMessage(), containsString("complete stored _source"));
    }
}
