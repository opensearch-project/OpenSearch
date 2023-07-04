/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.tracing;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import org.opensearch.common.settings.Settings;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.test.OpenSearchIntegTestCase;


import org.opensearch.telemetry.OTelTelemetryPlugin;

import static org.opensearch.index.query.QueryBuilders.queryStringQuery;

import org.opensearch.plugins.Plugin;
import org.opensearch.client.Client;
import org.opensearch.test.telemetry.tracing.TelemetryValidators;
import org.opensearch.test.telemetry.tracing.validators.*;

import java.lang.Thread;
import java.util.*;
import java.util.Collections;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class RTCTracingEnabledSanityIT extends OpenSearchIntegTestCase {
    private static MockOpenTelemetrySpanExporter exporter = new MockOpenTelemetrySpanExporter();

    static{
        OTelResourceProvider.get(Settings.builder().build(), exporter, ContextPropagators.create(W3CTraceContextPropagator.getInstance()), null);
    }
    @Override
    protected Class<? extends Plugin> telemetryPlugin() {
        return OTelTelemetryPlugin.class;
    }

    public void testSanityChecksWhenTracingEnabled() throws Exception{
        Client client = client();
        // ENABLE TRACING
        client
            .admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true))
            .get();

        //Create Index and ingest data
        String indexName = "test-index-11";
        Settings basicSettings = Settings.builder().put("number_of_shards", 3).put("number_of_replicas", 0).build();
        createIndex(indexName, basicSettings);
        indexRandom(true, client.prepareIndex(indexName).setId("1").setSource("field1", "the fox jumps in the well"));
        indexRandom(true, client.prepareIndex(indexName).setId("1").setSource("field2", "another fox did the same."));

        ensureGreen();
        refresh();

        //Make the search call;
        client.prepareSearch().setQuery(queryStringQuery("fox")).get();

        //Sleep for about 2s to wait for traces are published
        Thread.sleep(2000);

        TelemetryValidators validators = new TelemetryValidators(Arrays.asList(
            AllSpansAreEndedProperly.class,
            AllSpansAreInOrder.class,
            AllSpansHaveUniqueId.class,
            NumberOfTraceIDsEqualToRequests.class,
            TotalParentSpansEqualToRequests.class));

        if (exporter.getFinishedSpanItems().size() != 0) {
            validators.validate(exporter.getFinishedSpanItems(), 1);
        }
    }

}
