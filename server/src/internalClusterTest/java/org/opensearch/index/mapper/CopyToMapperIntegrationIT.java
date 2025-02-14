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

package org.opensearch.index.mapper;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.Aggregator.SubAggCollectionMode;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class CopyToMapperIntegrationIT extends OpenSearchIntegTestCase {
    public void testDynamicTemplateCopyTo() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test-idx").setMapping(createDynamicTemplateMapping()));

        int recordCount = between(1, 200);

        for (int i = 0; i < recordCount * 2; i++) {
            client().prepareIndex("test-idx").setId(Integer.toString(i)).setSource("test_field", "test " + i, "even", i % 2 == 0).get();
        }
        client().admin().indices().prepareRefresh("test-idx").execute().actionGet();

        SubAggCollectionMode aggCollectionMode = randomFrom(SubAggCollectionMode.values());

        SearchResponse response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.termQuery("even", true))
            .addAggregation(AggregationBuilders.terms("test").field("test_field").size(recordCount * 2).collectMode(aggCollectionMode))
            .addAggregation(
                AggregationBuilders.terms("test_raw").field("test_field_raw").size(recordCount * 2).collectMode(aggCollectionMode)
            )
            .execute()
            .actionGet();

        assertThat(response.getHits().getTotalHits().value(), equalTo((long) recordCount));

        assertThat(((Terms) response.getAggregations().get("test")).getBuckets().size(), equalTo(recordCount + 1));
        assertThat(((Terms) response.getAggregations().get("test_raw")).getBuckets().size(), equalTo(recordCount));

    }

    public void testDynamicObjectCopyTo() throws Exception {
        String mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("foo")
            .field("type", "text")
            .field("copy_to", "root.top.child")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertAcked(client().admin().indices().prepareCreate("test-idx").setMapping(mapping));
        client().prepareIndex("test-idx").setId("1").setSource("foo", "bar").get();
        client().admin().indices().prepareRefresh("test-idx").execute().actionGet();
        SearchResponse response = client().prepareSearch("test-idx").setQuery(QueryBuilders.termQuery("root.top.child", "bar")).get();
        assertThat(response.getHits().getTotalHits().value(), equalTo(1L));
    }

    private XContentBuilder createDynamicTemplateMapping() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startArray("dynamic_templates")

            .startObject()
            .startObject("template_raw")
            .field("match", "*_raw")
            .field("match_mapping_type", "string")
            .startObject("mapping")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()

            .startObject()
            .startObject("template_all")
            .field("match", "*")
            .field("match_mapping_type", "string")
            .startObject("mapping")
            .field("type", "text")
            .field("fielddata", true)
            .field("copy_to", "{name}_raw")
            .endObject()
            .endObject()
            .endObject()

            .endArray()
            .endObject();
    }

}
