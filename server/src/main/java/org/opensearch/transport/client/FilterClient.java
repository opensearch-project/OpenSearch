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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.support.AbstractClient;

/**
 * A {@link Client} that contains another {@link Client} which it
 * uses as its basic source, possibly transforming the requests / responses along the
 * way or providing additional functionality.
 *
 * @opensearch.internal
 */
public abstract class FilterClient extends AbstractClient {

    protected final Client in;

    /**
     * Creates a new FilterClient
     *
     * @param in the client to delegate to
     * @see #in()
     */
    public FilterClient(Client in) {
        this(in.settings(), in.threadPool(), in);
    }

    /**
     * A Constructor that allows to pass settings and threadpool separately. This is useful if the
     * client is a proxy and not yet fully constructed ie. both dependencies are not available yet.
     */
    protected FilterClient(Settings settings, ThreadPool threadPool, Client in) {
        super(settings, threadPool);
        this.in = in;
    }

    @Override
    public void close() {
        in().close();
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        in().execute(action, request, listener);
    }

    /**
     * Returns the delegate {@link Client}
     */
    protected Client in() {
        return in;
    }

    @Override
    public Client getRemoteClusterClient(String clusterAlias) {
        return in.getRemoteClusterClient(clusterAlias);
    }
}
