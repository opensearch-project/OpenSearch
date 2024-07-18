/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.rest.action.admin.cluster.RestClusterStatsAction;
import org.opensearch.rest.action.admin.cluster.RestNodesInfoAction;
import org.opensearch.rest.action.admin.cluster.RestNodesStatsAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.threadpool.TestThreadPool;
import org.junit.After;

import java.io.IOException;
import java.util.Collections;

public class RestStatsActionTests extends OpenSearchTestCase {
    private final TestThreadPool threadPool = new TestThreadPool(RestStatsActionTests.class.getName());
    private final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);

    @After
    public void terminateThreadPool() {
        terminate(threadPool);
    }

    public void testClusterStatsActionPrepareRequestNoError() throws IOException {
        RestClusterStatsAction action = new RestClusterStatsAction();
        action.prepareRequest(new FakeRestRequest(), client);
    }

    public void testNodesStatsActionPrepareRequestNoError() throws IOException {
        RestNodesStatsAction action = new RestNodesStatsAction();
        action.prepareRequest(new FakeRestRequest(), client);
    }

    public void testNodesInfoActionPrepareRequestNoError() throws IOException {
        RestNodesInfoAction action = new RestNodesInfoAction(new SettingsFilter(Collections.singleton("foo.filtered")));
        action.prepareRequest(new FakeRestRequest(), client);
    }
}
