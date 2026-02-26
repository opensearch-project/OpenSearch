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

import org.opensearch.OpenSearchException;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

public class FetchSourceContextTests extends OpenSearchTestCase {

    private XContentParser createSourceParser(XContentBuilder source) throws IOException {
        XContentParser parser = createParser(source);
        parser.nextToken(); // move to start object
        parser.nextToken(); // move to field name "_source"
        parser.nextToken(); // move to _source value to parse
        return parser;
    }

    public void testFetchSource() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("_source", true).endObject();
        final XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertEquals(FetchSourceContext.FETCH_SOURCE, result);
    }

    public void testDoNotFetchSource() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("_source", false).endObject();
        final XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertEquals(FetchSourceContext.DO_NOT_FETCH_SOURCE, result);
    }

    public void testFetchSourceString() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("_source", "include1").endObject();
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
        final XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("_source").startArray().endArray().endObject();
        final XContentParser parser = createSourceParser(source);

        ParsingException result = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        assertEquals(
            "Expected at least one value for an array of [" + FetchSourceContext.INCLUDES_FIELD.getPreferredName() + "]",
            result.getMessage()
        );
    }

    public void testFetchSourceAsObject() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("_source")
            .startObject()
            .field("includes", "include1")
            .field("excludes", "exclude1")
            .endObject()
            .endObject();
        final XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertTrue(result.fetchSource()); // fetch source
        assertArrayEquals(new String[] { "include1" }, result.includes()); // single include
        assertArrayEquals(new String[] { "exclude1" }, result.excludes()); // single exclude
    }

    public void testFetchSourceAsObjectBothIncludeAndExcludeArrays() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject()
            .field("_source")
            .startObject()
            .field("includes")
            .startArray()
            .value("iii")
            .endArray()
            .field("excludes")
            .startArray()
            .value("aaa")
            .value("bbb")
            .endArray()
            .endObject()
            .endObject();
        final XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertTrue(result.fetchSource());
        assertArrayEquals(new String[] { "iii" }, result.includes());
        assertArrayEquals(new String[] { "aaa", "bbb" }, result.excludes());
    }

    public void testFetchSourceObjectEmptyObjectNotAllowed() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("_source").startObject().endObject().endObject();
        final XContentParser parser = createSourceParser(source);

        ParsingException result = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
        assertEquals(
            "Expected at least one of ["
                + FetchSourceContext.INCLUDES_FIELD.getPreferredName()
                + "] or ["
                + FetchSourceContext.EXCLUDES_FIELD.getPreferredName()
                + "]",
            result.getMessage()
        );
    }

    public void testFetchSourceObjectExplicitEmptyArraysNotAllowed() throws IOException {
        {
            final XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("_source")
                .startObject()
                .field("includes", "include1")
                .field("excludes")
                .startArray()
                .endArray()
                .endObject()
                .endObject();
            final XContentParser parser = createSourceParser(source);

            ParsingException result = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertEquals(
                "Expected at least one value for an array of [" + FetchSourceContext.EXCLUDES_FIELD.getPreferredName() + "]",
                result.getMessage()
            );
        }
        {
            final XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("_source")
                .startObject()
                .field("excludes")
                .startArray()
                .value("exclude1")
                .endArray()
                .field("includes")
                .startArray()
                .endArray()
                .endObject()
                .endObject();
            final XContentParser parser = createSourceParser(source);

            ParsingException result = expectThrows(ParsingException.class, () -> FetchSourceContext.fromXContent(parser));
            assertEquals(
                "Expected at least one value for an array of [" + FetchSourceContext.INCLUDES_FIELD.getPreferredName() + "]",
                result.getMessage()
            );
        }
    }

    public void testFetchSourceAsObjectConflictingEntries() throws IOException {
        {
            final XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("_source")
                .startObject()
                .field("includes")
                .value("AAA")
                .field("excludes")
                .value("AAA")
                .endObject()
                .endObject();
            final XContentParser parser = createSourceParser(source);

            OpenSearchException result = expectThrows(OpenSearchException.class, () -> FetchSourceContext.fromXContent(parser));
            assertEquals("The same entry [AAA] cannot be both included and excluded in _source.", result.getMessage());
        }
        {
            final XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("_source")
                .startObject()
                .field("includes")
                .value("AAA")
                .field("excludes")
                .startArray()
                .value("AAA")
                .value("BBB")
                .endArray()
                .endObject()
                .endObject();
            final XContentParser parser = createSourceParser(source);

            OpenSearchException result = expectThrows(OpenSearchException.class, () -> FetchSourceContext.fromXContent(parser));
            assertEquals("The same entry [AAA] cannot be both included and excluded in _source.", result.getMessage());
        }
        {
            final XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("_source")
                .startObject()
                .field("includes")
                .startArray()
                .value("AAA")
                .value("BBB")
                .endArray()
                .field("excludes")
                .value("AAA")
                .endObject()
                .endObject();
            final XContentParser parser = createSourceParser(source);

            OpenSearchException result = expectThrows(OpenSearchException.class, () -> FetchSourceContext.fromXContent(parser));
            assertEquals("The same entry [AAA] cannot be both included and excluded in _source.", result.getMessage());
        }
        {
            final XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .field("_source")
                .startObject()
                .field("includes")
                .startArray()
                .value("AAA")
                .value("BBB")
                .endArray()
                .field("excludes")
                .startArray()
                .value("BBB")
                .value("AAA")
                .endArray()
                .endObject()
                .endObject();
            final XContentParser parser = createSourceParser(source);

            OpenSearchException result = expectThrows(OpenSearchException.class, () -> FetchSourceContext.fromXContent(parser));
            assertEquals("The same entry [BBB] cannot be both included and excluded in _source.", result.getMessage());
        }
    }

    public void testParseSourceObjectInvalidInput() throws IOException {
        final XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("_source", true).endObject();
        final XContentParser parser = createSourceParser(source);

        ParsingException result = expectThrows(ParsingException.class, () -> FetchSourceContext.parseSourceObject(parser));
        assertEquals("Expected a START_OBJECT but got a VALUE_BOOLEAN in [_source].", result.getMessage());
    }
}
