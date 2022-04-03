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

package org.opensearch.action.search;

import org.opensearch.Version;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.util.concurrent.AtomicArray;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.shard.ShardId;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;

public class SearchContextIdTests extends OpenSearchTestCase {

    QueryBuilder randomQueryBuilder() {
        if (randomBoolean()) {
            return new TermQueryBuilder(randomAlphaOfLength(10), randomAlphaOfLength(10));
        } else if (randomBoolean()) {
            return new MatchAllQueryBuilder();
        } else {
            return new IdsQueryBuilder().addIds(randomAlphaOfLength(10));
        }
    }

    public void testEncode() {
        final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, MatchAllQueryBuilder.NAME, MatchAllQueryBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, IdsQueryBuilder.NAME, IdsQueryBuilder::new)
            )
        );
        final AtomicArray<SearchPhaseResult> queryResults = TransportSearchHelperTests.generateQueryResults();
        final Version version = Version.CURRENT;
        final Map<String, AliasFilter> aliasFilters = new HashMap<>();
        for (SearchPhaseResult result : queryResults.asList()) {
            final AliasFilter aliasFilter;
            if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder());
            } else if (randomBoolean()) {
                aliasFilter = new AliasFilter(randomQueryBuilder(), "alias-" + between(1, 10));
            } else {
                aliasFilter = AliasFilter.EMPTY;
            }
            if (randomBoolean()) {
                aliasFilters.put(result.getSearchShardTarget().getShardId().getIndex().getUUID(), aliasFilter);
            }
        }
        final String id = SearchContextId.encode(queryResults.asList(), aliasFilters, version);
        final SearchContextId context = SearchContextId.decode(namedWriteableRegistry, id);
        assertThat(context.shards().keySet(), hasSize(3));
        assertThat(context.aliasFilter(), equalTo(aliasFilters));
        SearchContextIdForNode node1 = context.shards().get(new ShardId("idx", "uuid1", 2));
        assertThat(node1.getClusterAlias(), equalTo("cluster_x"));
        assertThat(node1.getNode(), equalTo("node_1"));
        assertThat(node1.getSearchContextId().getId(), equalTo(1L));
        assertThat(node1.getSearchContextId().getSessionId(), equalTo("a"));

        SearchContextIdForNode node2 = context.shards().get(new ShardId("idy", "uuid2", 42));
        assertThat(node2.getClusterAlias(), equalTo("cluster_y"));
        assertThat(node2.getNode(), equalTo("node_2"));
        assertThat(node2.getSearchContextId().getId(), equalTo(12L));
        assertThat(node2.getSearchContextId().getSessionId(), equalTo("b"));

        SearchContextIdForNode node3 = context.shards().get(new ShardId("idy", "uuid2", 43));
        assertThat(node3.getClusterAlias(), nullValue());
        assertThat(node3.getNode(), equalTo("node_3"));
        assertThat(node3.getSearchContextId().getId(), equalTo(42L));
        assertThat(node3.getSearchContextId().getSessionId(), equalTo("c"));
    }
}
