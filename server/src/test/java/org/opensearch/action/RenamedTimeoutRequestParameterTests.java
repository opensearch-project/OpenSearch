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
import org.opensearch.rest.action.cat.RestNodesAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;

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
     * Validate both parameters 'cluster_manager_timeout' and its predecessor can be parsed correctly.
     */
    public void testCatAllocation() {
        RestNodesAction action = new RestNodesAction();
        // Request with only new parameter will be parsed without warning and exception.
        action.doCatRequest(getRestRequestWithNewParam(), client);
        // Request with only deprecated parameter will result deprecation warning.
        action.doCatRequest(getRestRequestWithDeprecatedParam(), client);
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
        // Request with both new and deprecated parameters and different values will result exception.
        // It should have warning, but the same deprecation warning won't be logged again.
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
