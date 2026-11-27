/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalTestCluster;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

/**
 * Restarts a node with Painless disabled while an index has a Painless script in its settings (scripted similarity).
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PainlessDisabledRestartIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(PainlessModulePlugin.class);
    }

    public void testRestartWithPainlessDisabledAndScriptedSimilarity() throws Exception {
        internalCluster().startNode();
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.similarity.my_sim.type", "scripted")
            .put("index.similarity.my_sim.script.source", "return query.boost * doc.freq;")
            .build();
        client().admin()
            .indices()
            .prepareCreate("scripted")
            .setSettings(indexSettings)
            .setMapping("{\"properties\":{\"body\":{\"type\":\"text\",\"similarity\":\"my_sim\"}}}")
            .get();
        client().prepareIndex("scripted")
            .setId("1")
            .setSource("{\"body\":\"hello world\"}", MediaTypeRegistry.JSON)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .get();
        ensureGreen("scripted");

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put("script.engines.painless.enabled", false).build();
            }
        });

        ClusterHealthResponse health = client().admin()
            .cluster()
            .prepareHealth("scripted")
            .setWaitForEvents(org.opensearch.common.Priority.LANGUID)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .setWaitForGreenStatus()
            .get();
        logger.info("--> health after restart with painless disabled: {}, timed out: {}", health.getStatus(), health.isTimedOut());
        assertEquals("index with existing scripted similarity must stay green", ClusterHealthStatus.GREEN, health.getStatus());
        assertEquals(1, client().prepareSearch("scripted").get().getHits().getTotalHits().value());

        // unrelated settings updates still work on the index that already has the script
        assertTrue(
            client().admin()
                .indices()
                .prepareUpdateSettings("scripted")
                .setSettings(Settings.builder().put("index.refresh_interval", "2s"))
                .get()
                .isAcknowledged()
        );
        // changing the existing script is a new definition and is rejected
        client().admin().indices().prepareClose("scripted").get();
        Exception updateError = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .prepareUpdateSettings("scripted")
                .setSettings(Settings.builder().put("index.similarity.my_sim.script.source", "return 2.0 * doc.freq;"))
                .get()
        );
        assertTrue(updateError.toString(), updateError.toString().contains("disabled by [script.engines.painless.enabled]"));
        client().admin().indices().prepareOpen("scripted").get();

        // new definitions using the disabled language are rejected
        Exception e = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareCreate("scripted_new").setSettings(indexSettings).get()
        );
        assertTrue(
            e.toString(),
            e.toString().contains("uses script language [painless], which is disabled by [script.engines.painless.enabled]")
        );
        Exception templateError = expectThrows(
            Exception.class,
            () -> client().admin()
                .indices()
                .preparePutTemplate("scripted_template")
                .setPatterns(List.of("tmpl-*"))
                .setSettings(indexSettings)
                .get()
        );
        assertTrue(templateError.toString(), templateError.toString().contains("disabled by [script.engines.painless.enabled]"));
    }

    public void testRestartWithPainlessDisabledAndContextAwareGroupingMapping() throws Exception {
        internalCluster().startNode();
        String mapping = """
            {
              "context_aware_grouping": {"fields": ["tenant"], "script": {"source": "return 'group';"}},
              "properties": {"tenant": {"type": "keyword"}}
            }""";
        client().admin()
            .indices()
            .prepareCreate("grouped")
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .setMapping(mapping)
            .get();
        ensureGreen("grouped");

        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put("script.engines.painless.enabled", false).build();
            }
        });
        ensureGreen("grouped");

        Exception e = expectThrows(
            Exception.class,
            () -> client().admin().indices().prepareCreate("grouped_new").setMapping(mapping).get()
        );
        assertTrue(e.toString(), e.toString().contains("[context_aware_grouping.script] uses script language [painless]"));
    }
}
