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

package org.elasticsearch.search.stats;


import org.opensearch.action.ActionFuture;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.aggregations.AggregationBuilders.count;
import static org.opensearch.test.OpenSearchIntegTestCase.*;


@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 3, numClientNodes = 1)
public class TaskResourceTrackerIT extends OpenSearchIntegTestCase {

    private void addDocuments(String index, int start, int end) throws Exception {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = start; i < end; i++) {
            builders.add(client().prepareIndex(index, "type", "" + i + 1).setSource(jsonBuilder()
                .startObject()
                .field("value", i + 1)
                .field("tag", "tag" + i)
                .endObject()));
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    public void testAggregation() throws Exception {
        String index = "idx";
        createIndex(index, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1).build());
        addDocuments(index, 0, 50);

        ActionFuture<SearchResponse> searchResponseActionFuture = client().prepareSearch(index)
            .setQuery(matchAllQuery())
            .addAggregation(count("count").field("value"))
            .execute();

        List<String> nodeIds = getNodeNames(index);

//        Thread.sleep(6000);

//        NodesStatsResponse nodesStats = client().admin().cluster().prepareNodesStats().addMetric("thread_pool/search").get();

        searchResponseActionFuture.get();
    }

    private List<String> getNodeNames(String indexName) {
        IndicesStatsResponse response = client().admin().indices().prepareStats(indexName).get();
        return Stream.of(response.getShards())
            .map(ShardStats::getShardRouting)
            .filter(ShardRouting::primary)
            .map(ShardRouting::currentNodeId)
            .collect(Collectors.toList());
    }

}
