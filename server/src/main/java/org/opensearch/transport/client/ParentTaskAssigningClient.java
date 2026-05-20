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

package org.opensearch.transport.client;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.Task;

/**
 * A {@linkplain Client} that sets the parent task on all requests that it makes. Use this to conveniently implement actions that cause
 * many other actions.
 *
 * @opensearch.internal
 */
public class ParentTaskAssigningClient extends FilterClient {
    private final TaskId parentTask;

    /**
     * Standard constructor.
     */
    public ParentTaskAssigningClient(Client in, TaskId parentTask) {
        super(in);
        this.parentTask = parentTask;
    }

    /**
     * Convenience constructor for building the TaskId out of what is usually at hand.
     */
    public ParentTaskAssigningClient(Client in, DiscoveryNode localNode, Task parentTask) {
        this(in, new TaskId(localNode.getId(), parentTask.getId()));
    }

    /**
     * Fetch the wrapped client. Use this to make calls that don't set {@link ActionRequest#setParentTask(TaskId)}.
     */
    public Client unwrap() {
        return in();
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        request.setParentTask(parentTask);
        super.doExecute(action, request, listener);
    }
}
