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

package org.opensearch.action.admin.indices.alias;

import org.opensearch.action.support.clustermanager.AcknowledgedRequestBuilder;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.index.query.QueryBuilder;

import java.util.Map;

/**
 * Builder for request to modify many aliases at once.
 *
 * @opensearch.internal
 */
public class IndicesAliasesRequestBuilder extends AcknowledgedRequestBuilder<
    IndicesAliasesRequest,
    AcknowledgedResponse,
    IndicesAliasesRequestBuilder> {

    public IndicesAliasesRequestBuilder(OpenSearchClient client, IndicesAliasesAction action) {
        super(client, action, new IndicesAliasesRequest());
    }

    /**
     * Adds an alias to the index.
     *
     * @param index The index
     * @param alias The alias
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index).alias(alias));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param indices The indices
     * @param alias   The alias
     */
    public IndicesAliasesRequestBuilder addAlias(String[] indices, String alias) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().indices(indices).alias(alias));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index  The index
     * @param alias  The alias
     * @param filter The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, String filter) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index).alias(alias).filter(filter));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param indices The indices
     * @param alias   The alias
     * @param filter  The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String indices[], String alias, String filter) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().indices(indices).alias(alias).filter(filter));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param indices The indices
     * @param alias   The alias
     * @param filter  The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String[] indices, String alias, Map<String, Object> filter) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().indices(indices).alias(alias).filter(filter));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index  The indices
     * @param alias  The alias
     * @param filter The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, Map<String, Object> filter) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index).alias(alias).filter(filter));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param indices       The indices
     * @param alias         The alias
     * @param filterBuilder The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String indices[], String alias, QueryBuilder filterBuilder) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().indices(indices).alias(alias).filter(filterBuilder));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index         The index
     * @param alias         The alias
     * @param filterBuilder The filter
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, QueryBuilder filterBuilder) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index).alias(alias).filter(filterBuilder));
        return this;
    }

    /**
     * Adds an alias to the index.
     *
     * @param index         The index
     * @param alias         The alias
     * @param writeIndex    write index flag
     */
    public IndicesAliasesRequestBuilder addAlias(String index, String alias, boolean writeIndex) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.add().index(index).alias(alias).writeIndex(writeIndex));
        return this;
    }

    /**
     * Removes an alias from the index.
     *
     * @param index The index
     * @param alias The alias
     */
    public IndicesAliasesRequestBuilder removeAlias(String index, String alias) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(index).alias(alias));
        return this;
    }

    /**
     * Removes aliases from the index.
     *
     * @param indices The indices
     * @param aliases The aliases
     */
    public IndicesAliasesRequestBuilder removeAlias(String[] indices, String... aliases) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.remove().indices(indices).aliases(aliases));
        return this;
    }

    /**
     * Removes aliases from the index.
     *
     * @param index   The index
     * @param aliases The aliases
     */
    public IndicesAliasesRequestBuilder removeAlias(String index, String[] aliases) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(index).aliases(aliases));
        return this;
    }

    public IndicesAliasesRequestBuilder removeIndex(String index) {
        request.addAliasAction(IndicesAliasesRequest.AliasActions.removeIndex().index(index));
        return this;
    }

    /**
     * Adds an alias action to the request.
     *
     * @param action The alias action
     */
    public IndicesAliasesRequestBuilder addAliasAction(IndicesAliasesRequest.AliasActions action) {
        request.addAliasAction(action);
        return this;
    }
}
