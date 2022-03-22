/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.OpenSearchParseException;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.action.cat.RestAllocationAction;
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
    private static final String PARAM_VALUE_ERROR_MESSAGE = "[master_timeout, cluster_manager_timeout] are required to be equal";

    /**
     * Validate both cluster_manager_timeout and its predecessor can be parsed correctly.
     */
    public void testCatAllocationTimeoutErrorAndWarning() {
        RestAllocationAction action = new RestAllocationAction();
        TestThreadPool threadPool = new TestThreadPool(RenamedTimeoutRequestParameterTests.class.getName());
        NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
        FakeRestRequest request = getFakeRestRequestWithBothOldAndNewTimeoutParam();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(request, client));
        assertThat(e.getMessage(), containsString(PARAM_VALUE_ERROR_MESSAGE));
        assertWarnings(RestAllocationAction.MASTER_TIMEOUT_DEPRECATED_MESSAGE);
        terminate(threadPool);
    }

    private FakeRestRequest getFakeRestRequestWithBothOldAndNewTimeoutParam() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", randomFrom("1h", "2m"));
        request.params().put("master_timeout", "3s");
        return request;
    }
}
