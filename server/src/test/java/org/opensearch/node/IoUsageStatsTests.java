/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Locale;

public class IoUsageStatsTests extends OpenSearchTestCase {
    IoUsageStats ioUsageStats;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ioUsageStats = new IoUsageStats(10);
    }

    public void testDefaultsIoUsageStats() {
        assertEquals(ioUsageStats.getIoUtilisationPercent(), 10.0, 0);
    }

    public void testUpdateIoUsageStats() {
        assertEquals(ioUsageStats.getIoUtilisationPercent(), 10.0, 0);
        ioUsageStats.setIoUtilisationPercent(20);
        assertEquals(ioUsageStats.getIoUtilisationPercent(), 20.0, 0);
    }

    public void testIoUsageStats() throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder = ioUsageStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String response = builder.toString();
        assertEquals(response, "{\"max_io_utilization_percent\":\"10.0\"}");
        ioUsageStats.setIoUtilisationPercent(20);
        builder = JsonXContent.contentBuilder();
        builder = ioUsageStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        response = builder.toString();
        assertEquals(response, "{\"max_io_utilization_percent\":\"20.0\"}");
    }

    public void testIoUsageStatsToString() {
        String expected = "IO utilization percent: " + String.format(Locale.ROOT, "%.1f", 10.0);
        assertEquals(expected, ioUsageStats.toString());
        ioUsageStats.setIoUtilisationPercent(20);
        expected = "IO utilization percent: " + String.format(Locale.ROOT, "%.1f", 20.0);
        assertEquals(expected, ioUsageStats.toString());
    }
}
