/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.plugins.Plugin;
import org.opensearch.telemetry.IntegrationTestOTelTelemetryPlugin;
import org.opensearch.telemetry.OTelTelemetrySettings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.index.query.QueryBuilders.queryStringQuery;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, supportsDedicatedMasters = false, minNumDataNodes = 2)
public class TelemetryTracerDisabledSanityIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                OTelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(),
                "org.opensearch.telemetry.tracing.InMemorySingletonSpanExporter"
            )
            .put(OTelTelemetrySettings.TRACER_EXPORTER_DELAY_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(IntegrationTestOTelTelemetryPlugin.class);
    }

    @Override
    protected boolean addMockTelemetryPlugin() {
        return false;
    }

    public void testSanityCheckWhenTracingDisabled() throws Exception {
        Client client = client();
        // DISABLE TRACING
        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), false))
            .get();

        // Create Index and ingest data
        String indexName = "test-index-11";
        Settings basicSettings = Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 1).build();
        createIndex(indexName, basicSettings);
        indexRandom(true, client.prepareIndex(indexName).setId("1").setSource("field1", "t`"));

        ensureGreen();
        refresh();
        InMemorySingletonSpanExporter exporter = InMemorySingletonSpanExporter.INSTANCE;
        exporter.reset();

        // Make the search call;
        client.prepareSearch().setQuery(queryStringQuery("fox")).get();

        // Sleep for about 3s to wait for traces are published (the delay is 1s)
        Thread.sleep(3000);

        assertTrue(exporter.getFinishedSpanItems().isEmpty());
    }

}
