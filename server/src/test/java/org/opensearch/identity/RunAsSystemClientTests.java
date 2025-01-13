/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.client.Client;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;

public class RunAsSystemClientTests extends OpenSearchSingleNodeTestCase {
    @Before
    public void setup() {
        client().threadPool().getThreadContext().stashContext(); // start with fresh context
    }

    public void testThatContextIsRestoredOnActionListenerResponse() throws Exception {
        Client systemClient = new RunAsSystemClient(client());
        systemClient.threadPool().getThreadContext().putHeader("test_header", "foo");

        systemClient.admin().cluster().health(new ClusterHealthRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                String testHeader = systemClient.threadPool().getThreadContext().getHeader("test_header");
                assertThat(testHeader, equalTo("foo"));
            }

            @Override
            public void onFailure(Exception e) {
                fail("Expected cluster health action to succeed");
            }
        });
    }

    public void testThatContextIsRestoredOnActionListenerFailure() throws Exception {
        Client systemClient = new RunAsSystemClient(client());
        systemClient.threadPool().getThreadContext().putHeader("test_header", "bar");

        systemClient.admin().cluster().health(new ClusterHealthRequest("dne"), new ActionListener<>() {
            @Override
            public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                fail("Expected cluster health action to fail");
            }

            @Override
            public void onFailure(Exception e) {
                String testHeader = systemClient.threadPool().getThreadContext().getHeader("test_header");
                assertThat(testHeader, equalTo("bar"));
            }
        });
    }
}
