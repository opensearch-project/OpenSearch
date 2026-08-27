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
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Unit tests for {@link RankDocsSortBuilder}. Like the {@code rank_docs} query, this sort spec is purely
 * internal to the retriever framework: it is built in-process and transported over the binary stream, and
 * can never be authored in a {@code _search} body. These tests pin that contract:
 * {@link RankDocsSortBuilder#fromXContent} always rejects, while binary serialization and programmatic
 * construction work. Mirrors {@link RankDocsQueryBuilderTests}.
 */
public class RankDocsSortBuilderTests extends OpenSearchTestCase {

    private static final String JSON =
        "{\"rank_docs_sort\":[{\"index\":\"products\",\"shard\":0,\"id\":\"a\",\"score\":0.9,\"position\":0}]}";

    /** Parser positioned at the START_OBJECT of the sort body, mirroring how the sort registry hands off. */
    private XContentParser parser(String json) throws Exception {
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        parser.nextToken(); // START_OBJECT
        parser.nextToken(); // FIELD_NAME rank_docs_sort
        return parser;
    }

    public void testFromXContentAlwaysRejects() throws Exception {
        // rank_docs_sort is internal-only: authoring it in a _search body must always fail, with a message naming the sort.
        ParsingException e = expectThrows(
            ParsingException.class,
            () -> RankDocsSortBuilder.fromXContent(parser(JSON), RankDocsSortBuilder.NAME)
        );
        assertTrue(
            "message should name the sort and explain it is internal, got: " + e.getMessage(),
            e.getMessage().contains(RankDocsSortBuilder.NAME) && e.getMessage().contains("internal")
        );
    }

    public void testStreamRoundTrip() throws Exception {
        // The path the coordinator -> data-node hop actually uses (readNamedWriteable -> StreamInput ctor).
        RankDocsSortBuilder original = new RankDocsSortBuilder(
            List.of(new RankDoc("products", 0, "a", 0.9f, 0), new RankDoc("reviews", 2, "b", 0.1f, 5))
        );
        RankDocsSortBuilder copy;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                copy = new RankDocsSortBuilder(in);
            }
        }
        assertEquals(original, copy);
    }

    public void testGetWriteableName() {
        RankDocsSortBuilder b = new RankDocsSortBuilder(List.of(new RankDoc("products", 0, "a", 0.9f, 0)));
        assertEquals(RankDocsSortBuilder.NAME, b.getWriteableName());
    }
}
