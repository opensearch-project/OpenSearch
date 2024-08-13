/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.http.executioncontextplugin.TestExecutionContextPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test to ensure that plugin execution context is set when plugin runs transport action with PluginSubject
 */
@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class ExecutionContextPluginGetIT extends HttpSmokeTestCase {

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

    public void testGetExecutionContext() throws IOException {
        ensureGreen();
        Response response = getRestClient().performRequest(new Request("GET", "/_get_execution_context"));
        String responseBody = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        assertThat(responseBody, containsString(TestExecutionContextPlugin.class.getName()));
    }
}
