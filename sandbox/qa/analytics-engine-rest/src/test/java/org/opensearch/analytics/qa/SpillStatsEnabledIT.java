/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Iterator;

/**
 * REST IT verifying spill stats when spill is enabled. Runs against the dedicated
 * {@code integTestSpillEnabled} test cluster, which sets {@code datafusion.spill_directory}
 * and {@code datafusion.spill_memory_limit_bytes}.
 *
 * <p>Default {@code integTest} excludes this class so the disabled-state assertions in
 * {@link DataFusionStatsRestIT} stay valid against the default cluster.
 */
public class SpillStatsEnabledIT extends OpenSearchRestTestCase {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String SPILL_ENDPOINT = "/_plugins/_analytics_backend_datafusion/stats/disk_spill";
    private static final long EXPECTED_RESERVED = 1_073_741_824L;

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    /**
     * Every node reports a non-empty directory, the configured reserved-bytes value,
     * and non-zero disk_total_bytes (since the spill dir is on a real filesystem).
     */
    public void testEnabledStateSurfacesConfiguredDirectoryAndReserve() throws Exception {
        Response response = client().performRequest(new Request("GET", SPILL_ENDPOINT));
        assertEquals(200, response.getStatusLine().getStatusCode());

        JsonNode root = parseResponse(response);
        JsonNode nodes = root.get("nodes");
        assertTrue("expected at least one node", nodes.size() > 0);

        Iterator<String> ids = nodes.fieldNames();
        int verified = 0;
        while (ids.hasNext()) {
            String id = ids.next();
            JsonNode spill = nodes.get(id).get("disk_spill");
            assertNotNull("node " + id + " missing spill section", spill);

            String dir = spill.get("directory").asText();
            assertFalse("directory must not be empty when spill is enabled (node " + id + ")", dir.isEmpty());

            assertEquals(
                "disk_reserved_bytes must equal datafusion.spill_memory_limit_bytes",
                EXPECTED_RESERVED,
                spill.get("disk_reserved_bytes").asLong()
            );

            long total = spill.get("disk_total_bytes").asLong();
            long available = spill.get("disk_available_bytes").asLong();
            long used = spill.get("disk_used_bytes").asLong();

            assertTrue("disk_total_bytes should be > 0 when spill dir is on a real FS", total > 0);
            assertTrue("disk_available_bytes should be > 0", available > 0);
            assertTrue("disk_used_bytes should be >= 0", used >= 0);
            // The collector's invariant: total = available + used.
            assertEquals("total = available + used invariant", total, available + used);

            verified++;
        }
        assertTrue("expected at least one node verified", verified > 0);
    }

    private JsonNode parseResponse(Response response) throws IOException, ParseException {
        return MAPPER.readTree(EntityUtils.toString(response.getEntity()));
    }
}
