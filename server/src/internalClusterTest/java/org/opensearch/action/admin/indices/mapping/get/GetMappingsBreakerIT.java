/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.action.admin.indices.mapping.get;

import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.client.Client;

import java.security.SecureRandom;
import java.util.Base64;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 1, scope = OpenSearchIntegTestCase.Scope.TEST)
public class GetMappingsBreakerIT extends OpenSearchIntegTestCase {

    // Keep the limit very small so the mapping easily exceeds it.
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Request breaker (used by TransportGetMappingsAction)
            .put("indices.breaker.request.limit", "128kb")
            // keep parent breaker high so it doesn't interfere
            .put("indices.breaker.total.limit", "2gb")
            .build();
    }

    public void testGetMappingsTripsRequestBreaker() throws Exception {
        final String index = "big-mappings";

        // Build a mapping with low-compressibility padding under _meta.
        // Using ~256KB of random base64.
        final String pad = randomBase64(256 * 1024);

        final XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_meta")
            .field("pad", pad)
            .endObject()
            .startObject("properties")
            .startObject("title")
            .field("type", "text")
            .endObject()
            .startObject("year")
            .field("type", "integer")
            .endObject()
            .endObject()
            .endObject();

        final Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        final Client client = client();

        // Create index with the mapping containing the big _meta pad
        CreateIndexRequestBuilder create = client.admin().indices().prepareCreate(index).setSettings(indexSettings).setMapping(mapping);
        assertAcked(create.get());

        ensureGreen(index);

        // Now call GET _mappings and expect the REQUEST breaker to trip.
        final GetMappingsRequest req = new GetMappingsRequest().indices(index);

        CircuitBreakingException cbe = expectThrows(
            CircuitBreakingException.class,
            () -> client.admin().indices().getMappings(req).actionGet()
        );

        assertThat(cbe.getMessage(), containsString("Data too large"));
    }

    private static String randomBase64(int approxBytes) {
        byte[] buf = new byte[approxBytes];
        new SecureRandom().nextBytes(buf);
        return Base64.getEncoder().encodeToString(buf);
    }
}
