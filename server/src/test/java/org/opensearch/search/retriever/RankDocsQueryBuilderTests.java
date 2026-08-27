/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.retriever;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link RankDocsQueryBuilder}. The {@code rank_docs} query is purely internal to the
 * retriever framework: it is built in-process on the coordinator and transported to data nodes over the
 * binary stream, and can never be authored in a {@code _search} body. These tests pin that contract:
 * {@link RankDocsQueryBuilder#fromXContent} always rejects, while binary serialization and programmatic
 * construction (the paths the framework actually uses) work.
 */
public class RankDocsQueryBuilderTests extends OpenSearchTestCase {

    private static final String RANK_DOCS_JSON =
        "{\"docs\":[{\"index\":\"products\",\"shard\":0,\"id\":\"a\",\"score\":0.9,\"position\":0}]}";

    /** A parser positioned at the start of the rank_docs body (mirrors how the query registry invokes fromXContent). */
    private XContentParser parser(String json) throws Exception {
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        // Advance to the first token (START_OBJECT) so fromXContent begins where the registry would hand off.
        parser.nextToken();
        return parser;
    }

    public void testFromXContentAlwaysRejects() throws Exception {
        // rank_docs is internal-only: authoring it in a _search body must always fail, with a message naming the query.
        ParsingException e = expectThrows(ParsingException.class, () -> RankDocsQueryBuilder.fromXContent(parser(RANK_DOCS_JSON)));
        assertTrue(
            "message should name the query and explain it is internal, got: " + e.getMessage(),
            e.getMessage().contains(RankDocsQueryBuilder.NAME) && e.getMessage().contains("internal")
        );
    }

    public void testFromXContentRejectsEvenWellFormedBody() throws Exception {
        // A structurally valid body (docs + boost + _name) is still rejected — there is no flag or path that admits it.
        String json = "{\"docs\":[{\"index\":\"products\",\"shard\":0,\"id\":\"a\",\"score\":0.9,\"position\":0}],"
            + "\"boost\":2.5,\"_name\":\"my_query\"}";
        expectThrows(ParsingException.class, () -> RankDocsQueryBuilder.fromXContent(parser(json)));
    }

    public void testProgrammaticConstruction() {
        // The internal retriever path builds the query directly; this must work unconditionally.
        RankDocsQueryBuilder builder = new RankDocsQueryBuilder(List.of(new RankDoc("products", 0, "a", 0.9f, 0)));
        assertEquals(RankDocsQueryBuilder.NAME, builder.getWriteableName());
    }

    public void testRankDocStreamRoundTrip() throws Exception {
        RankDoc original = new RankDoc("products", 3, "abc", 0.42f, 7);
        RankDoc copy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                copy = new RankDoc(in);
            }
        }
        assertEquals(original, copy);
    }

    public void testBuilderStreamRoundTrip() throws Exception {
        // This is the path the coordinator -> data-node hop actually uses (readNamedWriteable -> StreamInput ctor).
        RankDocsQueryBuilder original = new RankDocsQueryBuilder(
            List.of(new RankDoc("products", 0, "a", 0.9f, 0), new RankDoc("reviews", 2, "b", 0.1f, 5))
        );
        RankDocsQueryBuilder copy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                copy = new RankDocsQueryBuilder(in);
            }
        }
        assertEquals(original, copy);
    }

    public void testRankDocRendersToXContent() throws Exception {
        // toXContent is retained for the profile API / debugging; verify it emits all five fields.
        RankDoc doc = new RankDoc("products", 3, "abc", 0.42f, 7);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            doc.toXContent(builder, ToXContent.EMPTY_PARAMS);
            String json = builder.toString();
            assertTrue(json, json.contains("\"index\":\"products\""));
            assertTrue(json, json.contains("\"shard\":3"));
            assertTrue(json, json.contains("\"id\":\"abc\""));
            assertTrue(json, json.contains("\"score\":0.42"));
            assertTrue(json, json.contains("\"position\":7"));
        }
    }
}
