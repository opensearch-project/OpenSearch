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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.query;

import org.opensearch.client.Client;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class QueryRewriteContext {
    private final NamedXContentRegistry xContentRegistry;
    private final NamedWriteableRegistry writeableRegistry;
    protected final Client client;
    protected final LongSupplier nowInMillis;
    private final List<BiConsumer<Client, ActionListener<?>>> asyncActions = new ArrayList<>();
    private final boolean validate;

    public QueryRewriteContext(
        NamedXContentRegistry xContentRegistry,
        NamedWriteableRegistry writeableRegistry,
        Client client,
        LongSupplier nowInMillis
    ) {
        this(xContentRegistry, writeableRegistry, client, nowInMillis, false);
    }

    public QueryRewriteContext(
        NamedXContentRegistry xContentRegistry,
        NamedWriteableRegistry writeableRegistry,
        Client client,
        LongSupplier nowInMillis,
        boolean validate
    ) {

        this.xContentRegistry = xContentRegistry;
        this.writeableRegistry = writeableRegistry;
        this.client = client;
        this.nowInMillis = nowInMillis;
        this.validate = validate;
    }

    /**
     * The registry used to build new {@link XContentParser}s. Contains registered named parsers needed to parse the query.
     */
    public NamedXContentRegistry getXContentRegistry() {
        return xContentRegistry;
    }

    /**
     * Returns the time in milliseconds that is shared across all resources involved. Even across shards and nodes.
     */
    public long nowInMillis() {
        return nowInMillis.getAsLong();
    }

    public NamedWriteableRegistry getWriteableRegistry() {
        return writeableRegistry;
    }

    /**
     * Returns an instance of {@link QueryShardContext} if available or null otherwise
     */
    public QueryShardContext convertToShardContext() {
        return null;
    }

    /**
     * Returns an instance of {@link QueryCoordinatorContext} if available or null otherwise
     * @return
     */
    public QueryCoordinatorContext convertToCoordinatorContext() {
        return null;
    }

    /**
     * Registers an async action that must be executed before the next rewrite round in order to make progress.
     * This should be used if a rewriteabel needs to fetch some external resources in order to be executed ie. a document
     * from an index.
     */
    public void registerAsyncAction(BiConsumer<Client, ActionListener<?>> asyncAction) {
        asyncActions.add(asyncAction);
    }

    /**
     * Returns <code>true</code> if there are any registered async actions.
     */
    public boolean hasAsyncActions() {
        return asyncActions.isEmpty() == false;
    }

    /**
     * Executes all registered async actions and notifies the listener once it's done. The value that is passed to the listener is always
     * <code>null</code>. The list of registered actions is cleared once this method returns.
     */
    public void executeAsyncActions(ActionListener listener) {
        if (asyncActions.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        CountDown countDown = new CountDown(asyncActions.size());
        ActionListener<?> internalListener = new ActionListener() {
            @Override
            public void onResponse(Object o) {
                if (countDown.countDown()) {
                    listener.onResponse(null);
                }
            }

            @Override
            public void onFailure(Exception e) {
                if (countDown.fastForward()) {
                    listener.onFailure(e);
                }
            }
        };
        // make a copy to prevent concurrent modification exception
        List<BiConsumer<Client, ActionListener<?>>> biConsumers = new ArrayList<>(asyncActions);
        asyncActions.clear();
        for (BiConsumer<Client, ActionListener<?>> action : biConsumers) {
            action.accept(client, internalListener);
        }
    }

    public boolean validate() {
        return validate;
    }
}
