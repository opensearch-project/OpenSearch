/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.telemetry.OTelTelemetryPlugin;
import org.opensearch.telemetry.OtelTelemetrySettings;
import org.opensearch.plugins.Plugin;
import org.opensearch.client.Client;
import org.opensearch.test.telemetry.tracing.TelemetryValidators;
import org.opensearch.test.telemetry.tracing.validators.AllSpansAreEndedProperly;
import org.opensearch.test.telemetry.tracing.validators.AllSpansAreInOrder;
import org.opensearch.test.telemetry.tracing.validators.AllSpansHaveUniqueId;
import org.opensearch.test.telemetry.tracing.validators.NumberOfTraceIDsEqualToRequests;
import org.opensearch.test.telemetry.tracing.validators.TotalParentSpansEqualToRequests;

import java.util.Arrays;

import static org.opensearch.index.query.QueryBuilders.queryStringQuery;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 2)
public class TelemetryTracerEnabledSanityIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                OtelTelemetrySettings.OTEL_TRACER_SPAN_EXPORTER_CLASS_SETTING.getKey(),
                "org.opensearch.telemetry.tracing.InMemorySpanExporter"
            )
            .build();
    }

    @Override
    protected Class<? extends Plugin> telemetryPlugin() {

        return OTelTelemetryPlugin.class;
    }

    public void testSanityChecksWhenTracingEnabled() throws Exception {
        Client client = client();
        // ENABLE TRACING
        client.admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true))
            .get();

        // Create Index and ingest data
        String indexName = "test-index-11";
        Settings basicSettings = Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 0).build();
        createIndex(indexName, basicSettings);
        indexRandom(true, client.prepareIndex(indexName).setId("1").setSource("field1", "the fox jumps in the well"));
        indexRandom(true, client.prepareIndex(indexName).setId("1").setSource("field2", "another fox did the same."));

        ensureGreen();
        refresh();

        // Make the search calls;
        client.prepareSearch().setQuery(queryStringQuery("fox")).get();
        client.prepareSearch().setQuery(queryStringQuery("jumps")).get();

        // Sleep for about 3s to wait for traces are published
        Thread.sleep(3000);

        TelemetryValidators validators = new TelemetryValidators(
            Arrays.asList(
                AllSpansAreEndedProperly.class,
                AllSpansAreInOrder.class,
                AllSpansHaveUniqueId.class,
                NumberOfTraceIDsEqualToRequests.class,
                TotalParentSpansEqualToRequests.class
            )
        );
        InMemorySpanExporter exporter = new InMemorySpanExporter();
        if (!exporter.getFinishedSpanItems().isEmpty()) {
            validators.validate(exporter.getFinishedSpanItems(), 2);
        }
    }

}
