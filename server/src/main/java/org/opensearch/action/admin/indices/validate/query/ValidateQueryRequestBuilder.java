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

package org.opensearch.action.admin.indices.validate.query;

import org.opensearch.action.support.broadcast.BroadcastOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.query.QueryBuilder;

/**
 * Transport Request Builder to Validate a Query
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ValidateQueryRequestBuilder extends BroadcastOperationRequestBuilder<
    ValidateQueryRequest,
    ValidateQueryResponse,
    ValidateQueryRequestBuilder> {

    public ValidateQueryRequestBuilder(OpenSearchClient client, ValidateQueryAction action) {
        super(client, action, new ValidateQueryRequest());
    }

    /**
     * The query to validate.
     *
     * @see org.opensearch.index.query.QueryBuilders
     */
    public ValidateQueryRequestBuilder setQuery(QueryBuilder queryBuilder) {
        request.query(queryBuilder);
        return this;
    }

    /**
     * Indicates if detailed information about the query should be returned.
     *
     * @see org.opensearch.index.query.QueryBuilders
     */
    public ValidateQueryRequestBuilder setExplain(boolean explain) {
        request.explain(explain);
        return this;
    }

    /**
     * Indicates whether the query should be rewritten into primitive queries
     */
    public ValidateQueryRequestBuilder setRewrite(boolean rewrite) {
        request.rewrite(rewrite);
        return this;
    }

    /**
     * Indicates whether the query should be validated on all shards
     */
    public ValidateQueryRequestBuilder setAllShards(boolean rewrite) {
        request.allShards(rewrite);
        return this;
    }
}
