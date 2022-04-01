/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.junit.After;
import org.opensearch.OpenSearchParseException;
import org.opensearch.action.support.master.MasterNodeRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.BaseRestHandler;
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
    private final TestThreadPool threadPool = new TestThreadPool(RenamedTimeoutRequestParameterTests.class.getName());
    private final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RenamedTimeoutRequestParameterTests.class);

    private static final String DUPLICATE_PARAMETER_ERROR_MESSAGE =
        "Please only use one of the request parameters [master_timeout, cluster_manager_timeout].";
    private static final String MASTER_TIMEOUT_DEPRECATED_MESSAGE =
        "Deprecated parameter [master_timeout] used. To promote inclusive language, please use [cluster_manager_timeout] instead. It will be unsupported in a future major version.";

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testNoWarningsForNewParam() {
        BaseRestHandler.parseDeprecatedMasterTimeoutParameter(
            getMasterNodeRequest(),
            getRestRequestWithNewParam(),
            deprecationLogger,
            "test"
        );
    }

    public void testDeprecationWarningForOldParam() {
        BaseRestHandler.parseDeprecatedMasterTimeoutParameter(
            getMasterNodeRequest(),
            getRestRequestWithDeprecatedParam(),
            deprecationLogger,
            "test"
        );
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testBothParamsNotValid() {
        Exception e = assertThrows(
            OpenSearchParseException.class,
            () -> BaseRestHandler.parseDeprecatedMasterTimeoutParameter(
                getMasterNodeRequest(),
                getRestRequestWithBothParams(),
                deprecationLogger,
                "test"
            )
        );
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    public void testCatAllocation() {
        RestNodesAction action = new RestNodesAction();
        Exception e = assertThrows(OpenSearchParseException.class, () -> action.doCatRequest(getRestRequestWithBothParams(), client));
        assertThat(e.getMessage(), containsString(DUPLICATE_PARAMETER_ERROR_MESSAGE));
        assertWarnings(MASTER_TIMEOUT_DEPRECATED_MESSAGE);
    }

    private MasterNodeRequest getMasterNodeRequest() {
        return new MasterNodeRequest() {
            @Override
            public ActionRequestValidationException validate() {
                return null;
            }
        };
    }

    private FakeRestRequest getRestRequestWithBothParams() {
        FakeRestRequest request = new FakeRestRequest();
        request.params().put("cluster_manager_timeout", "1h");
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
