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
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.geo.builders.EnvelopeBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;

import org.locationtech.jts.geom.Coordinate;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;

public class ExternalValuesMapperIntegrationIT extends OpenSearchIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ExternalMapperPlugin.class);
    }

    public void testHighlightingOnCustomString() throws Exception {
        prepareCreate("test-idx").setMapping(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("field")
                .field("type", FakeStringFieldMapper.CONTENT_TYPE)
                .endObject()
                .endObject()
                .endObject()
        ).execute().get();

        index(
            "test-idx",
            "type",
            "1",
            XContentFactory.jsonBuilder().startObject().field("field", "Every day is exactly the same").endObject()
        );
        refresh();

        SearchResponse response;
        // test if the highlighting is excluded when we use wildcards
        response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.matchQuery("field", "exactly the same"))
            .highlighter(new HighlightBuilder().field("*"))
            .execute()
            .actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().size(), equalTo(0));

        // make sure it is not excluded when we explicitly provide the fieldname
        response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.matchQuery("field", "exactly the same"))
            .highlighter(new HighlightBuilder().field("field"))
            .execute()
            .actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().size(), equalTo(1));
        assertThat(
            response.getHits().getAt(0).getHighlightFields().get("field").fragments()[0].string(),
            equalTo("Every day is " + "<em>exactly</em> <em>the</em> <em>same</em>")
        );

        // make sure it is not excluded when we explicitly provide the fieldname and a wildcard
        response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.matchQuery("field", "exactly the same"))
            .highlighter(new HighlightBuilder().field("*").field("field"))
            .execute()
            .actionGet();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
        assertThat(response.getHits().getAt(0).getHighlightFields().size(), equalTo(1));
        assertThat(
            response.getHits().getAt(0).getHighlightFields().get("field").fragments()[0].string(),
            equalTo("Every day is " + "<em>exactly</em> <em>the</em> <em>same</em>")
        );
    }

    public void testExternalValues() throws Exception {
        prepareCreate("test-idx").setMapping(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject(ExternalMetadataMapper.CONTENT_TYPE)
                .endObject()
                .startObject("properties")
                .startObject("field")
                .field("type", ExternalMapperPlugin.EXTERNAL)
                .endObject()
                .endObject()
                .endObject()
        ).execute().get();

        index("test-idx", "type", "1", XContentFactory.jsonBuilder().startObject().field("field", "1234").endObject());
        refresh();

        SearchResponse response;

        response = client().prepareSearch("test-idx").setPostFilter(QueryBuilders.termQuery("field.bool", "true")).execute().actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));

        response = client().prepareSearch("test-idx")
            .setPostFilter(QueryBuilders.geoDistanceQuery("field.point").point(42.0, 51.0).distance("1km"))
            .execute()
            .actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));

        response = client().prepareSearch("test-idx")
            .setPostFilter(
                QueryBuilders.geoShapeQuery("field.shape", new EnvelopeBuilder(new Coordinate(-101, 46), new Coordinate(-99, 44)))
                    .relation(ShapeRelation.WITHIN)
            )
            .execute()
            .actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));

        response = client().prepareSearch("test-idx").setPostFilter(QueryBuilders.termQuery("field.field", "foo")).execute().actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));
    }

    public void testExternalValuesWithMultifield() throws Exception {
        prepareCreate("test-idx").setMapping(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("f")
                .field("type", ExternalMapperPlugin.EXTERNAL_UPPER)
                .startObject("fields")
                .startObject("g")
                .field("type", "text")
                .field("store", true)
                .startObject("fields")
                .startObject("raw")
                .field("type", "keyword")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        ).execute().get();

        index("test-idx", "_doc", "1", "f", "This is my text");
        refresh();

        SearchResponse response = client().prepareSearch("test-idx")
            .setQuery(QueryBuilders.termQuery("f.g.raw", "FOO BAR"))
            .execute()
            .actionGet();

        assertThat(response.getHits().getTotalHits().value, equalTo((long) 1));
    }
}
