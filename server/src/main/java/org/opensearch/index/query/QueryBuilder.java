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

import org.apache.lucene.search.Query;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.xcontent.ToXContentObject;

import java.io.IOException;

/**
 * Foundation class for all OpenSearch query builders
 *
 * @opensearch.internal
 */
public interface QueryBuilder extends NamedWriteable, ToXContentObject, Rewriteable<QueryBuilder> {

    /**
     * Converts this QueryBuilder to a lucene {@link Query}.
     * Returns {@code null} if this query should be ignored in the context of
     * parent queries.
     *
     * @param context additional information needed to construct the queries
     * @return the {@link Query} or {@code null} if this query should be ignored upstream
     */
    Query toQuery(QueryShardContext context) throws IOException;

    /**
     * Sets the arbitrary name to be assigned to the query (see named queries).
     * Implementers should return the concrete type of the
     * {@link QueryBuilder} so that calls can be chained. This is done
     * automatically when extending {@link AbstractQueryBuilder}.
     */
    QueryBuilder queryName(String queryName);

    /**
     * Returns the arbitrary name assigned to the query (see named queries).
     */
    String queryName();

    /**
     * Returns the boost for this query.
     */
    float boost();

    /**
     * Sets the boost for this query.  Documents matching this query will (in addition to the normal
     * weightings) have their score multiplied by the boost provided.
     * Implementers should return the concrete type of the
     * {@link QueryBuilder} so that calls can be chained. This is done
     * automatically when extending {@link AbstractQueryBuilder}.
     */
    QueryBuilder boost(float boost);

    /**
     * Returns the name that identifies uniquely the query
     */
    String getName();

    /**
     * Rewrites this query builder into its primitive form. By default this method return the builder itself. If the builder
     * did not change the identity reference must be returned otherwise the builder will be rewritten infinitely.
     */
    @Override
    default QueryBuilder rewrite(QueryRewriteContext queryShardContext) throws IOException {
        return this;
    }
}
