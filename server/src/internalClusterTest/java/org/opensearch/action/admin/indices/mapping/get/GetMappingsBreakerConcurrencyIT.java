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
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(numDataNodes = 1, scope = OpenSearchIntegTestCase.Scope.TEST)
public class GetMappingsBreakerConcurrencyIT extends OpenSearchIntegTestCase {

    // For this suite: allow a single request, but keep the parent breaker tight so many concurrent in-flight requests trip it.
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            // Keep request breaker above a single mapping response size
            .put("indices.breaker.request.limit", "512kb")
            // Tight parent breaker so concurrent requests can exceed it
            .put("indices.breaker.total.limit", "768kb")
            .build();
    }

    public void testSingleGetMappingsDoesNotTrip() throws Exception {
        final String index = "big-mappings-ok";

        // ~256KB of low-compressibility pad under _meta to inflate the response materially,
        // but still below the 512kb request breaker.
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

        CreateIndexRequestBuilder create = client.admin().indices().prepareCreate(index).setSettings(indexSettings).setMapping(mapping);
        assertAcked(create.get());
        ensureGreen(index);

        // Should not trip.
        client.admin().indices().getMappings(new GetMappingsRequest().indices(index)).actionGet();
    }

    public void testManyConcurrentGetMappingsTripParentBreaker() throws Exception {
        final String index = "big-mappings-concurrent";

        // Each request fits under request breaker, but a burst of them should exceed the 768kb parent limit.
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

        CreateIndexRequestBuilder create = client.admin().indices().prepareCreate(index).setSettings(indexSettings).setMapping(mapping);
        assertAcked(create.get());
        ensureGreen(index);

        final int threads = 8; // small node, but enough to blow past ~768kb with in-flight results
        final CountDownLatch ready = new CountDownLatch(threads);
        final CountDownLatch startGun = new CountDownLatch(1);
        final List<Thread> workers = new ArrayList<>(threads);
        final AtomicReference<CircuitBreakingException> anyCBE = new AtomicReference<>();

        for (int i = 0; i < threads; i++) {
            final Thread t = new Thread(() -> {
                try {
                    ready.countDown();
                    startGun.await();
                    // Fire several requests per thread in quick succession to increase overlap
                    for (int j = 0; j < 3; j++) {
                        try {
                            client.admin().indices().getMappings(new GetMappingsRequest().indices(index)).actionGet();
                        } catch (CircuitBreakingException cbe) {
                            anyCBE.compareAndSet(null, cbe);
                            // Stop this worker early after a breaker trip to reduce noise
                            break;
                        }
                    }
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            });
            workers.add(t);
            t.start();
        }

        // synchronize the start so requests overlap
        ready.await();
        startGun.countDown();

        for (Thread t : workers) {
            t.join();
        }

        final CircuitBreakingException cbe = anyCBE.get();
        assertNotNull("Expected at least one GET _mappings to trip the parent breaker under concurrent load", cbe);
        assertThat(cbe.getMessage(), containsString("Data too large"));
    }

    private static String randomBase64(int approxBytes) {
        byte[] buf = new byte[approxBytes];
        new SecureRandom().nextBytes(buf);
        return Base64.getEncoder().encodeToString(buf);
    }
}
