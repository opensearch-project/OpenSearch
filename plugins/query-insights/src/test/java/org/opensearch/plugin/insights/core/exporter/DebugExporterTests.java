/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.exporter;

import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.List;

/**
 * Granular tests for the {@link DebugExporterTests} class.
 */
public class DebugExporterTests extends OpenSearchTestCase {
    private DebugExporter debugExporter;

    @Before
    public void setup() {
        debugExporter = DebugExporter.getInstance();
    }

    public void testExport() {
        List<SearchQueryRecord> records = QueryInsightsTestUtils.generateQueryInsightRecords(2);
        try {
            debugExporter.export(records);
        } catch (Exception e) {
            fail("No exception should be thrown when exporting query insights data");
        }
    }
}
