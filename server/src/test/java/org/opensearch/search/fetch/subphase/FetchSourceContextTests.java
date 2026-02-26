/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase.HitContext;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// ISSUE-20612 Source Validation
public class FetchSourceContextTests extends OpenSearchTestCase {

    private XContentParser createSourceParser(XContentBuilder source) throws IOException {
        XContentParser parser = createParser(source);
        parser.nextToken(); // move to start object
        parser.nextToken(); // move to field name "_source"
        parser.nextToken(); // move to _source value to parse
        return parser;
    }

    public void testFetchSource() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject().field("_source", true).endObject();
        final XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertEquals(FetchSourceContext.FETCH_SOURCE, result);
    }

    public void testDoNotFetchSource() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject().field("_source", false).endObject();
        final XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertEquals(FetchSourceContext.DO_NOT_FETCH_SOURCE, result);
    }

    public void testFetchSourceString() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("_source", "include1")
            .endObject();
        final XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertTrue(result.fetchSource()); // fetch source
        assertArrayEquals(new String[] { "include1" }, result.includes()); // single include
        assertEquals(0, result.excludes().length); // no excludes
    }

    public void testFetchSourceArray() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("_source")
            .startArray()
            .value("include1")
            .value("include2")
            .value("include2")
            .endArray()
            .endObject();
        final XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertTrue(result.fetchSource()); // fetch source
        assertArrayEquals(new String[] { "include1", "include2" }, result.includes()); // no duplicates
        assertEquals(0, result.excludes().length); // no excludes
    }

    public void testFetchSourceExplicitEmptyArrayNotAllowed() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("_source")
            .startArray()
            .endArray()
            .endObject();
        final XContentParser parser = createSourceParser(source);

        ParsingException result = expectThrows(ParsingException.class,
            () -> FetchSourceContext.fromXContent(parser)
        );
        assertEquals("Expected at least one value for an array of [" + FetchSourceContext.INCLUDES_FIELD.getPreferredName() + "]", result.getMessage());
    }
}
