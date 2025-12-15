/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.netty4;

import org.opensearch.OpenSearchNetty4IntegTestCase;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.rest.action.RestCancellableNodeClient;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, supportsDedicatedMasters = false, numDataNodes = 1)
public class Netty4HttpChannelsReleaseIntegTests extends OpenSearchNetty4IntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void stopThreadPool() {
        ThreadPool.terminate(threadPool, 5, TimeUnit.SECONDS);
    }

    public void testAcceptedChannelsGetCleanedUpOnTheNodeShutdown() throws InterruptedException {
        String testIndex = "test_idx";
        assertAcked(client().admin().indices().prepareCreate(testIndex));

        int initialHttpChannels = RestCancellableNodeClient.getNumChannels();
        int numChannels = randomIntBetween(50, 100);
        CountDownLatch countDownLatch = new CountDownLatch(numChannels);
        for (int i = 0; i < numChannels; i++) {
            threadPool.generic().execute(() -> {
                executeRequest(testIndex);
                countDownLatch.countDown();
            });
        }
        countDownLatch.await();

        // no channels get closed in this test, hence we expect as many channels as we created in the map at most
        // it is difficult to match the exact number of HTTP channels since:
        // - there is 10 connections per route default setting (RestClient)
        // - there are at least 2 nodes in the cluster (master + data)
        // - if additional plugins are used (like telemetry), the same HTTP channel may
        // be wrapped around, inflating the number of HTTP channels being tracked
        assertThat(
            "All channels remain open",
            RestCancellableNodeClient.getNumChannels(),
            greaterThanOrEqualTo(initialHttpChannels + 10 /* default connections per route */)
        );
    }

    /**
     * Execute a Search request against the given index. The Search requests are tracked
     * by the RestCancellableNodeClient to verify that channels are released properly.
     *
     * @param index the index to search against
     */
    private static void executeRequest(String index) {
        try {
            Request request = new Request("GET", "/" + index + "/_search");
            SearchSourceBuilder searchSource = new SearchSourceBuilder().query(new MatchAllQueryBuilder());
            request.setJsonEntity(Strings.toString(MediaTypeRegistry.JSON, searchSource));
            Response response = getRestClient().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), anyOf(equalTo(200), equalTo(201)));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to execute the request", e);
        }
    }
}
