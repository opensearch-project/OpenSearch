/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort;

import org.apache.lucene.search.SortField;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class ShardDocSortBuilderTests extends AbstractSortTestCase<ShardDocSortBuilder> {

    @Override
    protected ShardDocSortBuilder createTestItem() {
        ShardDocSortBuilder b = new ShardDocSortBuilder();
        if (randomBoolean()) {
            b.order(randomFrom(SortOrder.values()));
        }
        return b;
    }

    @Override
    protected ShardDocSortBuilder mutate(ShardDocSortBuilder original) throws IOException {
        // only mutates order; builder is intentionally tiny
        ShardDocSortBuilder copy = new ShardDocSortBuilder(original);
        copy.order(original.order() == SortOrder.ASC ? SortOrder.DESC : SortOrder.ASC);
        return copy;
    }

    @Override
    protected void sortFieldAssertions(ShardDocSortBuilder builder, SortField sortField, DocValueFormat format) throws IOException {
        assertEquals(SortField.Type.CUSTOM, sortField.getType());
        assertEquals(builder.order() == SortOrder.DESC, sortField.getReverse());
        assertEquals(DocValueFormat.RAW, format);
    }

    @Override
    protected ShardDocSortBuilder fromXContent(XContentParser parser, String fieldName) throws IOException {
        return ShardDocSortBuilder.fromXContent(parser, fieldName);
    }

    public void testParseScalarAndObject() throws IOException {
        String json = "  [ { \"_shard_doc\": { \"order\": \"desc\" } } ] ";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        SortBuilder<?> sb = SortBuilder.fromXContent(parser).get(0);
        assertThat(sb, instanceOf(ShardDocSortBuilder.class));
        assertEquals(SortOrder.DESC, sb.order());

        json = "  [ { \"_shard_doc\": { \"order\": \"asc\" } } ] ";
        parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        sb = SortBuilder.fromXContent(parser).get(0);
        assertThat(sb, instanceOf(ShardDocSortBuilder.class));
        assertEquals(SortOrder.ASC, sb.order());

        json = "  [ \"_shard_doc\" ] "; // default to asc
        parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken();
        sb = SortBuilder.fromXContent(parser).get(0);
        assertThat(sb, instanceOf(ShardDocSortBuilder.class));
        assertEquals(SortOrder.ASC, sb.order());

        // from ShardDocSortBuilder
        json = "{ \"_shard_doc\": { \"order\": \"desc\" } }";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, json)) {
            p.nextToken();
            p.nextToken();
            p.nextToken();
            ShardDocSortBuilder b = ShardDocSortBuilder.fromXContent(p, "_shard_doc");
            assertEquals(SortOrder.DESC, b.order());
        }
    }

    public void testUnknownOptionFails() throws IOException {
        String json = "{ \"_shard_doc\": { \"reverse\": true } }";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, json)) {
            p.nextToken();
            p.nextToken();
            p.nextToken();
            XContentParseException e = expectThrows(XContentParseException.class, () -> ShardDocSortBuilder.fromXContent(p, "_shard_doc"));
            assertThat(e.getMessage(), containsString("unknown field [reverse]"));
        }
    }

    public void testXContentRoundTripAndSerialization() throws IOException {
        ShardDocSortBuilder original = new ShardDocSortBuilder().order(SortOrder.DESC);
        // XContent round-trip
        XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String rendered = builder.toString();
        SearchSourceBuilder ssb = SearchSourceBuilder.fromXContent(
            createParser(JsonXContent.jsonXContent, "{ \"sort\": [" + rendered + "] }")
        );
        SortBuilder<?> parsed = ssb.sorts().get(0);
        assertEquals(original, parsed);
        assertEquals(original.hashCode(), parsed.hashCode());

        // Stream serialization
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        ShardDocSortBuilder restored = new ShardDocSortBuilder(out.bytes().streamInput());
        assertEquals(original, restored);
    }
}
