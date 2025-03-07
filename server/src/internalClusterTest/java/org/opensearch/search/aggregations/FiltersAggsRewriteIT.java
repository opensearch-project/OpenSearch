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

package org.opensearch.search.aggregations;

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.filter.InternalFilters;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FiltersAggsRewriteIT extends OpenSearchSingleNodeTestCase {

    public void testWrapperQueryIsRewritten() throws IOException {
        createIndex("test", Settings.EMPTY, "test", "title", "type=text");
        client().prepareIndex("test").setId("1").setSource("title", "foo bar baz").get();
        client().prepareIndex("test").setId("2").setSource("title", "foo foo foo").get();
        client().prepareIndex("test").setId("3").setSource("title", "bar baz bax").get();
        client().admin().indices().prepareRefresh("test").get();

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference bytesReference;
        try (XContentBuilder builder = MediaTypeRegistry.contentBuilder(xContentType)) {
            builder.startObject();
            {
                builder.startObject("terms");
                {
                    builder.array("title", "foo");
                }
                builder.endObject();
            }
            builder.endObject();
            bytesReference = BytesReference.bytes(builder);
        }
        FiltersAggregationBuilder builder = new FiltersAggregationBuilder(
            "titles",
            new FiltersAggregator.KeyedFilter("titleterms", new WrapperQueryBuilder(bytesReference))
        );
        Map<String, Object> metadata = new HashMap<>();
        metadata.put(randomAlphaOfLengthBetween(1, 20), randomAlphaOfLengthBetween(1, 20));
        builder.setMetadata(metadata);
        SearchResponse searchResponse = client().prepareSearch("test").setSize(0).addAggregation(builder).get();
        assertEquals(3, searchResponse.getHits().getTotalHits().value());
        InternalFilters filters = searchResponse.getAggregations().get("titles");
        assertEquals(1, filters.getBuckets().size());
        assertEquals(2, filters.getBuckets().get(0).getDocCount());
        assertEquals(metadata, filters.getMetadata());
    }
}
