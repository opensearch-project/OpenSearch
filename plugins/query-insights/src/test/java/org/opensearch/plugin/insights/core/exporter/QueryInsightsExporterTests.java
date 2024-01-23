/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.plugin.insights.rules.model.SearchQueryLatencyRecord;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit Tests for {@link QueryInsightsExporter}.
 */
public class QueryInsightsExporterTests extends OpenSearchTestCase {
    public class DummyExporter<T extends SearchQueryRecord<?>> extends QueryInsightsExporter<T> {
        DummyExporter(String identifier) {
            super(identifier);
        }

        @Override
        public void export(List<T> records) {}
    }

    public void testGetType() {
        DummyExporter<SearchQueryLatencyRecord> exporter = new DummyExporter<>("test-index");
        String identifier = exporter.getIdentifier();
        assertEquals("test-index", identifier);
    }
}
