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
import org.opensearch.rest.action.cat.RestAllocationAction;
import org.opensearch.rest.action.cat.RestRepositoriesAction;
import org.opensearch.rest.action.cat.RestThreadPoolAction;
import org.opensearch.rest.action.cat.RestMasterAction;
import org.opensearch.rest.action.cat.RestShardsAction;
import org.opensearch.rest.action.cat.RestPluginsAction;
import org.opensearch.rest.action.cat.RestNodeAttrsAction;
import org.opensearch.rest.action.cat.RestNodesAction;
import org.opensearch.rest.action.cat.RestIndicesAction;
import org.opensearch.rest.action.cat.RestTemplatesAction;
import org.opensearch.rest.action.cat.RestPendingClusterTasksAction;
import org.opensearch.rest.action.cat.RestSegmentsAction;
import org.opensearch.rest.action.cat.RestSnapshotAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;

import static org.hamcrest.CoreMatchers.containsString;

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
    public void testCatAllocation() {
        RestAllocationAction action = new RestAllocationAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatIndices() {
        RestIndicesAction action = new RestIndicesAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatClusterManager() {
        RestMasterAction action = new RestMasterAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatNodeattrs() {
        RestNodeAttrsAction action = new RestNodeAttrsAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatNodes() {
        RestNodesAction action = new RestNodesAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatPendingTasks() {
        RestPendingClusterTasksAction action = new RestPendingClusterTasksAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatPlugins() {
        RestPluginsAction action = new RestPluginsAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatRepositories() {
        RestRepositoriesAction action = new RestRepositoriesAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatShards() {
        RestShardsAction action = new RestShardsAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatSnapshots() {
        RestSnapshotAction action = new RestSnapshotAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatTemplates() {
        RestTemplatesAction action = new RestTemplatesAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatThreadPool() {
        RestThreadPoolAction action = new RestThreadPoolAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
    }

    public void testCatSegments() {
        RestSegmentsAction action = new RestSegmentsAction();

        action.doCatRequest(getRestRequestWithNewParam(), client);

        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);

        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithWrongValues(), client));
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
