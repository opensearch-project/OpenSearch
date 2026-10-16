/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm;

import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.rule.RuleFrameworkPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Exercises the production plugin wiring through HTTP without replacing WLM's services or registries. */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, supportsDedicatedMasters = false, numClientNodes = 0)
public class WlmPluginIntegrationIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(Netty4ModulePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        // Unlike nodePlugins(), descriptors preserve the extension relationship used in production.
        return List.of(
            plugin("rule-framework", RuleFrameworkPlugin.class),
            plugin("workload-management", WorkloadManagementPlugin.class, "rule-framework")
        );
    }

    private static PluginInfo plugin(String name, Class<? extends Plugin> pluginClass, String... extendedPlugins) {
        return new PluginInfo(
            name,
            "integration test",
            "test",
            Version.CURRENT,
            "21",
            pluginClass.getName(),
            null,
            List.of(extendedPlugins),
            false
        );
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Netty's processor count is JVM-wide; keep it consistent across fresh test clusters.
            .put("node.processors", 1)
            .put("http.type", Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            .put("wlm.workload_group.mode", "enabled")
            .put("wlm.rule.sync_refresh_interval_ms", 1000)
            .build();
    }

    public void testSearchIsTaggedUsingPersistedRule() throws Exception {
        Map<String, Object> group = request("PUT", "/_wlm/workload_group", """
            {"name":"analytics","resiliency_mode":"monitor","resource_limits":{"cpu":0.4,"memory":0.4}}
            """);
        String groupId = (String) group.get("_id");
        assertNotNull(groupId);

        request("PUT", "/_rules/workload_group", String.format(Locale.ROOT, """
            {"description":"Tag analytics searches","index_pattern":["analytics-*"],"workload_group":"%s"}
            """, groupId));
        request("PUT", "/analytics-events", """
            {"settings":{"number_of_shards":1,"number_of_replicas":0}}
            """);
        request("PUT", "/analytics-events/_doc/1?refresh=true", """
            {"message":"example"}
            """);
        request("PUT", "/other-events", """
            {"settings":{"number_of_shards":1,"number_of_replicas":0}}
            """);

        // Rule persistence and synchronization are asynchronous. Poll the externally observable behavior.
        assertBusy(() -> {
            long before = completions(groupId);
            request("GET", "/analytics-events/_search", null);
            assertTrue("Matching search should complete in the selected workload group", completions(groupId) > before);
        }, 30, TimeUnit.SECONDS);

        long before = completions(groupId);
        request("GET", "/other-events/_search", null);
        assertEquals("Nonmatching search must not use the analytics group", before, completions(groupId));
    }

    @SuppressWarnings("unchecked")
    private long completions(String groupId) throws IOException {
        Map<String, Object> stats = request("GET", "/_wlm/stats/" + groupId, null);
        long completions = 0;
        for (Object value : stats.values()) {
            if (value instanceof Map<?, ?> node && node.get("workload_groups") instanceof Map<?, ?> groups) {
                Map<String, Object> group = (Map<String, Object>) groups.get(groupId);
                if (group != null) {
                    completions += ((Number) group.get("total_completions")).longValue();
                }
            }
        }
        return completions;
    }

    private Map<String, Object> request(String method, String path, String body) throws IOException {
        Request request = new Request(method, path);
        if (body != null) {
            request.setJsonEntity(body);
        }
        Response response = getRestClient().performRequest(request);
        assertTrue(response.getStatusLine().getStatusCode() < 300);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()))) {
            return parser.map();
        } catch (org.apache.hc.core5.http.ParseException e) {
            throw new IOException(e);
        }
    }
}
