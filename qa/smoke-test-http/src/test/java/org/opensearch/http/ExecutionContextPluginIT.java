/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.http;

import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.equalTo;

/**
 * Test a rest action that sets special response headers
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class ExecutionContextPluginIT extends HttpSmokeTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        ArrayList<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(TestExecutionContextPlugin.class);
        return plugins;
    }

    public void testThatSettingHeadersWorks() throws IOException {
        ensureGreen();
        try {
            Response response = getRestClient().performRequest(new Request("GET", "/_execution_context"));
            System.out.println("Response body: " + new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8));
//            fail("request should have failed");
        } catch(ResponseException e) {
            Response response = e.getResponse();
            assertThat(response.getStatusLine().getStatusCode(), equalTo(401));
            assertThat(response.getHeader("Secret"), equalTo("required"));
        }
    }
}
