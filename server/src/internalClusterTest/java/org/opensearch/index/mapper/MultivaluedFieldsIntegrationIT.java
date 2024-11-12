/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

public class MultivaluedFieldsIntegrationIT extends OpenSearchIntegTestCase {
    public void testMultivaluedFields() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("my-index-multivalued").setMapping(createMultivaluedTypeSource()));
        XContentBuilder singleValueSource = XContentFactory.jsonBuilder().startObject().field("title", "Hello world").endObject();
        assertThat(
            client().prepareBulk()
                .add(client().prepareIndex().setIndex("my-index-multivalued").setSource(singleValueSource))
                .get()
                .hasFailures(),
            equalTo(true)
        );
        XContentBuilder multiValueSource = XContentFactory.jsonBuilder()
            .startObject()
            .field("title", List.of("Hello world", "abcdef"))
            .endObject();
        assertThat(
            client().prepareBulk()
                .add(client().prepareIndex().setIndex("my-index-multivalued").setSource(multiValueSource))
                .get()
                .hasFailures(),
            equalTo(false)
        );
    }

    public void testGeoPointMultivaluedField() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("my-index-multivalued").setMapping(createMappingSource("geo_point")));
        XContentBuilder singleValueSource = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("a")
            .field("lat", 40.71)
            .field("lon", 74.0)
            .endObject()
            .endObject();
        assertThat(
            client().prepareBulk()
                .add(client().prepareIndex().setIndex("my-index-multivalued").setSource(singleValueSource))
                .get()
                .hasFailures(),
            equalTo(true)
        );
        XContentBuilder multiValueSource = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("a")
            .startObject()
            .field("lat", 40.71)
            .field("lon", 74.0)
            .endObject()
            .startObject()
            .field("lat", 63.45)
            .field("lon", 123.79)
            .endObject()
            .endArray()
            .endObject();
        assertThat(
            client().prepareBulk()
                .add(client().prepareIndex().setIndex("my-index-multivalued").setSource(multiValueSource))
                .get()
                .hasFailures(),
            equalTo(false)
        );
    }

    public void testCompletionMultivaluedField() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("my-index-multivalued").setMapping(createMappingSource("completion")));
        XContentBuilder singleValueSource = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("a")
            .array("input", "foo", "bar")
            .field("weight", 10)
            .endObject()
            .endObject();
        assertThat(
            client().prepareBulk()
                .add(client().prepareIndex().setIndex("my-index-multivalued").setSource(singleValueSource))
                .get()
                .hasFailures(),
            equalTo(true)
        );
        XContentBuilder multiValueSource = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("a")
            .startObject()
            .array("input", "foo", "bar")
            .field("weight", 10)
            .endObject()
            .startObject()
            .array("input", "baz", "xyz")
            .field("weight", 10)
            .endObject()
            .endArray()
            .endObject();
        assertThat(
            client().prepareBulk()
                .add(client().prepareIndex().setIndex("my-index-multivalued").setSource(multiValueSource))
                .get()
                .hasFailures(),
            equalTo(false)
        );
    }

    public void testIpMultivaluedField() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("my-index-multivalued").setMapping(createMappingSource("ip")));
        XContentBuilder singleValueSource = XContentFactory.jsonBuilder().startObject().field("a", "127.0.0.1").endObject();
        assertThat(
            client().prepareBulk()
                .add(client().prepareIndex().setIndex("my-index-multivalued").setSource(singleValueSource))
                .get()
                .hasFailures(),
            equalTo(true)
        );
        XContentBuilder multiValueSource = XContentFactory.jsonBuilder().startObject().array("a", "127.0.0.1", "127.0.0.1").endObject();
        assertThat(
            client().prepareBulk()
                .add(client().prepareIndex().setIndex("my-index-multivalued").setSource(multiValueSource))
                .get()
                .hasFailures(),
            equalTo(false)
        );
    }

    private XContentBuilder createMultivaluedTypeSource() throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("title")
            .field("type", "text")
            .field("multivalued", true)
            .startObject("fields")
            .startObject("not_analyzed")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();
    }

    private XContentBuilder createMappingSource(String fieldType) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("a")
            .field("type", fieldType)
            .field("multivalued", true)
            .endObject()
            .endObject()
            .endObject();
    }
}
