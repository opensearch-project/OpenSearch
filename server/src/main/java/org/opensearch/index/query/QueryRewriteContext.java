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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.transport.client.Client;

import java.util.function.BiConsumer;

/**
 * Context object used to rewrite {@link QueryBuilder} instances into simplified version.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface QueryRewriteContext {
    /**
     * The registry used to build new {@link XContentParser}s. Contains registered named parsers needed to parse the query.
     */
    NamedXContentRegistry getXContentRegistry();

    /**
     * Returns the time in milliseconds that is shared across all resources involved. Even across shards and nodes.
     */
    long nowInMillis();

    NamedWriteableRegistry getWriteableRegistry();

    /**
     * Returns an instance of {@link QueryShardContext} if available of null otherwise
     */
    default QueryShardContext convertToShardContext() {
        return null;
    }

    default QueryCoordinatorContext convertToCoordinatorContext() {
        return null;
    }

    /**
     * Registers an async action that must be executed before the next rewrite round in order to make progress.
     * This should be used if a rewriteabel needs to fetch some external resources in order to be executed ie. a document
     * from an index.
     */
    void registerAsyncAction(BiConsumer<Client, ActionListener<?>> asyncAction);

    /**
     * Returns <code>true</code> if there are any registered async actions.
     */
    boolean hasAsyncActions();

    /**
     * Executes all registered async actions and notifies the listener once it's done. The value that is passed to the listener is always
     * <code>null</code>. The list of registered actions is cleared once this method returns.
     */
    void executeAsyncActions(ActionListener listener);

    boolean validate();
}
