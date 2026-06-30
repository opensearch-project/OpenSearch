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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.indices;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class TermsLookupTests extends OpenSearchTestCase {
    public void testTermsLookup() {
        String index = randomAlphaOfLengthBetween(1, 10);
        String id = randomAlphaOfLengthBetween(1, 10);
        String path = randomAlphaOfLengthBetween(1, 10);
        String routing = randomAlphaOfLengthBetween(1, 10);
        boolean store = randomBoolean();
        TermsLookup termsLookup = new TermsLookup(index, id, path);
        termsLookup.routing(routing);
        termsLookup.store(store);
        assertEquals(index, termsLookup.index());
        assertEquals(id, termsLookup.id());
        assertEquals(path, termsLookup.path());
        assertEquals(routing, termsLookup.routing());
        assertEquals(store, termsLookup.store());
    }

    public void testIllegalArgumentsWithBasicConstructor() {
        String validIndex = randomAlphaOfLength(5);
        String validId = randomAlphaOfLength(5);
        String validPath = randomAlphaOfLength(5);

        // Null index
        IllegalArgumentException e1 = expectThrows(IllegalArgumentException.class, () -> new TermsLookup(null, validId, validPath));
        assertThat(e1.getMessage(), containsString("[terms] index cannot be null or empty"));

        // Null path
        IllegalArgumentException e2 = expectThrows(IllegalArgumentException.class, () -> new TermsLookup(validIndex, validId, null));
        assertThat(e2.getMessage(), containsString("[terms] path cannot be null or empty"));

        // Null id (query is implicitly null via 3-arg constructor)
        IllegalArgumentException e3 = expectThrows(IllegalArgumentException.class, () -> new TermsLookup(validIndex, null, validPath));
        assertThat(e3.getMessage(), containsString("[terms] query lookup element requires specifying either the id or the query"));
    }

    public void testIllegalArgumentsWithExtendedConstructor() {
        String validIndex = randomAlphaOfLength(5);
        String validId = randomAlphaOfLength(5);
        String validPath = randomAlphaOfLength(5);
        QueryBuilder validQuery = new MatchAllQueryBuilder();

        // Null index
        IllegalArgumentException e1 = expectThrows(
            IllegalArgumentException.class,
            () -> new TermsLookup(null, null, validPath, validQuery)
        );
        assertThat(e1.getMessage(), containsString("[terms] index cannot be null or empty"));

        // Null path
        IllegalArgumentException e2 = expectThrows(
            IllegalArgumentException.class,
            () -> new TermsLookup(validIndex, null, null, validQuery)
        );
        assertThat(e2.getMessage(), containsString("[terms] path cannot be null or empty"));

        // Both id and query null
        IllegalArgumentException e3 = expectThrows(
            IllegalArgumentException.class,
            () -> new TermsLookup(validIndex, null, validPath, null)
        );
        assertThat(e3.getMessage(), containsString("[terms] query lookup element requires specifying either the id or the query"));

        // Both id and query provided (invalid)
        IllegalArgumentException e4 = expectThrows(
            IllegalArgumentException.class,
            () -> new TermsLookup(validIndex, validId, validPath, validQuery)
        );
        assertThat(e4.getMessage(), containsString("[terms] query lookup element cannot specify both id and query."));
    }

    public void testSerialization() throws IOException {
        TermsLookup termsLookup = randomTermsLookup();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            termsLookup.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                TermsLookup deserializedLookup = new TermsLookup(in);
                assertEquals(deserializedLookup, termsLookup);
                assertEquals(deserializedLookup.hashCode(), termsLookup.hashCode());
                assertNotSame(deserializedLookup, termsLookup);
            }
        }
    }

    public void testXContentParsing() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{ \"index\" : \"index\", \"id\" : \"id\", \"path\" : \"path\", \"routing\" : \"routing\" }"
        );

        TermsLookup tl = TermsLookup.parseTermsLookup(parser);
        assertEquals("index", tl.index());
        assertEquals("id", tl.id());
        assertEquals("path", tl.path());
        assertEquals("routing", tl.routing());
    }

    public static TermsLookup randomTermsLookup() {
        return new TermsLookup(randomAlphaOfLength(10), randomAlphaOfLength(10), randomAlphaOfLength(10).replace('.', '_')).routing(
            randomBoolean() ? randomAlphaOfLength(10) : null
        ).store(randomBoolean());
    }

    public void testQuerySetterCoversChainAndNull() {
        // Covers: public TermsLookup query(QueryBuilder query)
        TermsLookup tl = new TermsLookup("idx", "docid", "path"); // id non-null
        QueryBuilder qb = new MatchAllQueryBuilder();
        TermsLookup returned = tl.query(qb);
        assertSame(tl, returned);
        assertEquals(qb, tl.query());

        // Setting null query is allowed, resets to null
        returned = tl.query(null);
        assertSame(tl, returned);
        assertNull(tl.query());
    }

    public void testSetQueryThrowsWhenIdPresent() {
        TermsLookup tl = new TermsLookup("idx", "docid", "path");
        QueryBuilder qb = new MatchAllQueryBuilder();
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> tl.setQuery(qb));
        assertThat(ex.getMessage(), containsString("query lookup element cannot specify both id and query"));
    }

    public void testSetQuerySetsWhenIdNull() {
        TermsLookup tl = new TermsLookup("idx", null, "path", new MatchAllQueryBuilder());
        // Setting another query is allowed if id is null
        QueryBuilder qb2 = new MatchAllQueryBuilder();
        tl.setQuery(qb2);
        assertEquals(qb2, tl.query());
    }

    public void testIdThrowsWhenQueryPresent() {
        TermsLookup tl = new TermsLookup("idx", null, "path", new MatchAllQueryBuilder());
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> tl.id("foo"));
        assertThat(ex.getMessage(), containsString("query lookup element cannot specify both id and query"));
    }

    public void testIdSetsWhenQueryNull() {
        TermsLookup tl = new TermsLookup("idx", "foo", "path"); // id is non-null, query null
        TermsLookup returned = tl.id("bar"); // allowed, updating id
        assertSame(tl, returned);
        assertEquals("bar", tl.id());
    }

    public void testToXContentWithQuery() throws IOException {
        TermsLookup tl = new TermsLookup("idx", null, "path", new MatchAllQueryBuilder());
        XContentBuilder builder = JsonXContent.contentBuilder().startObject();
        tl.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        String output = builder.toString();
        assertThat(output, containsString("\"query\""));
        assertThat(output, containsString("match_all"));
    }
}
