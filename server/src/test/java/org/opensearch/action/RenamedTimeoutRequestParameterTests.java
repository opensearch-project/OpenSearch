/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.junit.AfterClass;
import org.opensearch.OpenSearchParseException;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.action.admin.cluster.RestClusterGetSettingsAction;
import org.opensearch.rest.action.admin.cluster.RestClusterHealthAction;
import org.opensearch.rest.action.admin.cluster.RestClusterRerouteAction;
import org.opensearch.rest.action.admin.cluster.RestClusterStateAction;
import org.opensearch.rest.action.admin.cluster.RestClusterUpdateSettingsAction;
import org.opensearch.rest.action.admin.cluster.RestPendingClusterTasksAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

/**
 * As of 2.0, the request parameter 'master_timeout' in all applicable REST APIs is deprecated,
 * and alternative parameter 'cluster_manager_timeout' is added.
 * The tests are used to validate the behavior about the renamed request parameter.
 * Remove the test after removing MASTER_ROLE and 'master_timeout'.
 */
public class RenamedTimeoutRequestParameterTests extends OpenSearchTestCase {
    private static final TestThreadPool threadPool = new TestThreadPool(RenamedTimeoutRequestParameterTests.class.getName());
    private final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

    private static final String PARAM_VALUE_ERROR_MESSAGE = "[master_timeout, cluster_manager_timeout] are required to be equal";
    private static final String MASTER_TIMEOUT_DEPRECATED_MESSAGE =
        "Deprecated parameter [master_timeout] used. To promote inclusive language, please use [cluster_manager_timeout] instead. It will be unsupported in a future major version.";

    @AfterClass
    public static void terminateThreadPool() {
        terminate(threadPool);
    }

    /**
     * Validate both cluster_manager_timeout and its predecessor can be parsed correctly.
     */
    public void testClusterHealth() {
        RestClusterHealthAction.fromRequest(getRestRequestWithNewParam());

        RestClusterHealthAction.fromRequest(getRestRequestWithDeprecatedParam());
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(
            OpenSearchParseException.class,
            () -> RestClusterHealthAction.fromRequest(getRestRequestWithWrongValues())
        );
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testClusterReroute() throws IOException {
        RestClusterRerouteAction action = new RestClusterRerouteAction(null);

        action.prepareRequest(getRestRequestWithNewParam(), client);

        action.prepareRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testClusterState() throws IOException {
        RestClusterStateAction action = new RestClusterStateAction(null);

        action.prepareRequest(getRestRequestWithNewParam(), client);

        action.prepareRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testClusterGetSettings() throws IOException {
        RestClusterGetSettingsAction action = new RestClusterGetSettingsAction(null, null, null);

        action.prepareRequest(getRestRequestWithNewParam(), client);

        action.prepareRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testClusterUpdateSettings() throws IOException {
        RestClusterUpdateSettingsAction action = new RestClusterUpdateSettingsAction();

        action.prepareRequest(getRestRequestWithNewParam(), client);

        action.prepareRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testClusterPendingTasks() throws IOException {
        RestPendingClusterTasksAction action = new RestPendingClusterTasksAction();

        action.prepareRequest(getRestRequestWithNewParam(), client);

        action.prepareRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.prepareRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    private FakeRestRequest getRestRequestWithWrongValues() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", randomFrom("1h", "2m"));
        request.params().put("master_timeout", "3s");
        return request;
    }

    private FakeRestRequest getRestRequestWithDeprecatedParam() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("master_timeout", "3s");
        return request;
    }

    private FakeRestRequest getRestRequestWithNewParam() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "2m");
        return request;
    }
}
