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
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;

import java.security.Principal;

import static org.hamcrest.Matchers.equalTo;

public class RunAsSubjectClientTests extends OpenSearchSingleNodeTestCase {

    private final Subject TEST_SUBJECT = new Subject() {
        @Override
        public Principal getPrincipal() {
            return new NamedPrincipal("testSubject");
        }
    };

    @Before
    public void setup() {
        client().threadPool().getThreadContext().stashContext(); // start with fresh context
    }

    public void testThatContextIsRestoredOnActionListenerResponse() throws Exception {
        client().threadPool().getThreadContext().putHeader("test_header", "foo");

        client().runAs(TEST_SUBJECT).admin().cluster().health(new ClusterHealthRequest(), new ActionListener<>() {
            @Override
            public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                String testHeader = client().threadPool().getThreadContext().getHeader("test_header");
                assertThat(testHeader, equalTo("foo"));
            }

            @Override
            public void onFailure(Exception e) {
                fail("Expected cluster health action to succeed");
            }
        });
    }

    public void testThatContextIsRestoredOnActionListenerFailure() throws Exception {
        client().threadPool().getThreadContext().putHeader("test_header", "bar");

        client().runAs(TEST_SUBJECT).admin().cluster().health(new ClusterHealthRequest("dne"), new ActionListener<>() {
            @Override
            public void onResponse(ClusterHealthResponse clusterHealthResponse) {
                fail("Expected cluster health action to fail");
            }

            @Override
            public void onFailure(Exception e) {
                String testHeader = client().threadPool().getThreadContext().getHeader("test_header");
                assertThat(testHeader, equalTo("bar"));
            }
        });
    }
}
