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

package org.opensearch.test.client;

import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.client.support.AbstractClient;

import java.util.concurrent.TimeUnit;

/**
 * Client that always responds with {@code null} to every request. Override {@link #doExecute(ActionType, ActionRequest, ActionListener)}
 * for testing.
 * <p>
 * See also {@link NoOpNodeClient} if you need to mock a {@link NodeClient}.
 */
public class NoOpClient extends AbstractClient {
    /**
     * Build with {@link ThreadPool}. This {@linkplain ThreadPool} is terminated on {@link #close()}.
     */
    public NoOpClient(ThreadPool threadPool) {
        super(Settings.EMPTY, threadPool);
    }

    /**
     * Create a new {@link TestThreadPool} for this client. This {@linkplain TestThreadPool} is terminated on {@link #close()}.
     */
    public NoOpClient(String testName) {
        super(Settings.EMPTY, new TestThreadPool(testName));
    }

    @Override
    protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
        ActionType<Response> action,
        Request request,
        ActionListener<Response> listener
    ) {
        listener.onResponse(null);
    }

    @Override
    public void close() {
        try {
            ThreadPool.terminate(threadPool(), 10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new OpenSearchException(e.getMessage(), e);
        }
    }
}
