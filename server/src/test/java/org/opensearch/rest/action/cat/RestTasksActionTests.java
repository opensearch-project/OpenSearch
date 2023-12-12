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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest.action.cat;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.is;

public class RestTasksActionTests extends OpenSearchTestCase {

    public void testConsumesParameters() throws Exception {
        RestTasksAction action = new RestTasksAction(() -> DiscoveryNodes.EMPTY_NODES);
        FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            MapBuilder.<String, String>newMapBuilder()
                .put("parent_task_id", "the node:3")
                .put("nodes", "node1,node2")
                .put("actions", "*")
                .map()
        ).build();
        FakeRestChannel fakeRestChannel = new FakeRestChannel(fakeRestRequest, false, 1);
        try (NoOpNodeClient nodeClient = buildNodeClient()) {
            action.handleRequest(fakeRestRequest, fakeRestChannel, nodeClient);
        }

        assertThat(fakeRestChannel.errors().get(), is(0));
        assertThat(fakeRestChannel.responses().get(), is(1));
    }

    private NoOpNodeClient buildNodeClient() {
        return new NoOpNodeClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) new ListTasksResponse(emptyList(), emptyList(), emptyList()));
            }
        };
    }
}
