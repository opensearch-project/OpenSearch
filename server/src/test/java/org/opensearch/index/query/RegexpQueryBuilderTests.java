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

package org.opensearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RegexpQueryBuilderTests extends AbstractQueryTestCase<RegexpQueryBuilder> {

    @Override
    protected RegexpQueryBuilder doCreateTestQueryBuilder() {
        RegexpQueryBuilder query = randomRegexpQuery();
        if (randomBoolean()) {
            List<RegexpFlag> flags = new ArrayList<>();
            int iter = randomInt(5);
            for (int i = 0; i < iter; i++) {
                // Exclude COMPLEMENT from random selection to avoid deprecation warnings
                RegexpFlag[] availableFlags = {
                    RegexpFlag.INTERSECTION,
                    RegexpFlag.EMPTY,
                    RegexpFlag.ANYSTRING,
                    RegexpFlag.INTERVAL,
                    RegexpFlag.NONE,
                    RegexpFlag.ALL };
                flags.add(randomFrom(availableFlags));
            }
            query.flags(flags.toArray(new RegexpFlag[0]));
        }
        if (randomBoolean()) {
            query.caseInsensitive(true);
        }
        if (randomBoolean()) {
            query.maxDeterminizedStates(randomInt(50000));
        }
        if (randomBoolean()) {
            query.rewrite(randomFrom(getRandomRewriteMethod()));
        }
        return query;
    }

    @Override
    protected Map<String, RegexpQueryBuilder> getAlternateVersions() {
        Map<String, RegexpQueryBuilder> alternateVersions = new HashMap<>();
        RegexpQueryBuilder regexpQuery = randomRegexpQuery();
        String contentString = String.format(Locale.ROOT, """
            {
                "regexp" : {
                    "%s" : "%s"
                }
            }""", regexpQuery.fieldName(), regexpQuery.value());
        alternateVersions.put(contentString, regexpQuery);
        return alternateVersions;
    }

    private static RegexpQueryBuilder randomRegexpQuery() {
        // mapped or unmapped fields
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, randomAlphaOfLengthBetween(1, 10));
        String value = randomAlphaOfLengthBetween(1, 10);
        return new RegexpQueryBuilder(fieldName, value);
    }

    @Override
    protected void doAssertLuceneQuery(RegexpQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(RegexpQuery.class));
        RegexpQuery regexpQuery = (RegexpQuery) query;

        String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
        assertThat(regexpQuery.getField(), equalTo(expectedFieldName));
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RegexpQueryBuilder(null, "text"));
        assertEquals("field name is null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new RegexpQueryBuilder("", "text"));
        assertEquals("field name is null or empty", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RegexpQueryBuilder("field", null));
        assertEquals("value cannot be null", e.getMessage());
    }

    public void testMaxDeterminizedStatesIsBounded() {
        // Guards against CVE-2026-63136: an unbounded max_determinized_states disables Lucene's
        // determinization safeguard and lets a crafted pattern exhaust the heap.
        RegexpQueryBuilder query = new RegexpQueryBuilder("field", ".*a.{30}");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> query.maxDeterminizedStates(Integer.MAX_VALUE)
        );
        assertThat(e.getMessage(), containsString("max_determinized_states cannot exceed"));

        e = expectThrows(
            IllegalArgumentException.class,
            () -> query.maxDeterminizedStates(RegexpQueryBuilder.MAX_DETERMINIZE_WORK_LIMIT + 1)
        );
        assertThat(e.getMessage(), containsString("max_determinized_states cannot exceed"));

        e = expectThrows(IllegalArgumentException.class, () -> query.maxDeterminizedStates(-1));
        assertThat(e.getMessage(), containsString("cannot be negative"));

        // The ceiling itself and typical values remain accepted.
        assertEquals(
            RegexpQueryBuilder.MAX_DETERMINIZE_WORK_LIMIT,
            query.maxDeterminizedStates(RegexpQueryBuilder.MAX_DETERMINIZE_WORK_LIMIT).maxDeterminizedStates()
        );
        assertEquals(20000, query.maxDeterminizedStates(20000).maxDeterminizedStates());
    }

    public void testMaxDeterminizedStatesFromJsonIsBounded() {
        // The bound must also apply when the value arrives via the REST/XContent parse path.
        String json = String.format(Locale.ROOT, """
            {
                "regexp" : {
                    "field" : {
                        "value" : ".*a.{30}",
                        "max_determinized_states" : 2147483647
                    }
                }
            }""");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parseQuery(json));
        assertThat(e.getMessage(), containsString("max_determinized_states cannot exceed"));
    }

    public void testMaxDeterminizedStatesFromStreamIsBounded() throws IOException {
        // Guards against CVE-2026-63136 on the transport deserialization path: a patched data node
        // must reject an out-of-bounds value serialized by an (unpatched) coordinating node instead
        // of feeding it to Lucene. The setter bound cannot be reached by serializing a real builder
        // (the setter itself blocks it), so we hand-craft the wire bytes with an oversized value.
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeFloat(AbstractQueryBuilder.DEFAULT_BOOST); // boost
        out.writeOptionalString(null);                      // queryName
        out.writeString("field");                           // fieldName
        out.writeString(".*a.{30}");                        // value
        out.writeVInt(RegexpQueryBuilder.DEFAULT_FLAGS_VALUE);
        out.writeVInt(Integer.MAX_VALUE);                   // maxDeterminizedStates (malicious)
        out.writeOptionalString(null);                      // rewrite
        out.writeBoolean(false);                            // caseInsensitive

        try (StreamInput in = out.bytes().streamInput()) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new RegexpQueryBuilder(in));
            assertThat(e.getMessage(), containsString("max_determinized_states cannot exceed"));
        }
    }

    public void testFromJson() throws IOException {
        String json = """
            {
              "regexp" : {
                "name.first" : {
                  "value" : "s.*y",
                  "flags_value" : 7,
                  "case_insensitive" : true,
                  "max_determinized_states" : 20000,
                  "boost" : 1.0
                }
              }
            }""";

        RegexpQueryBuilder parsed = (RegexpQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "s.*y", parsed.value());
        assertEquals(json, 20000, parsed.maxDeterminizedStates());
    }

    public void testNumeric() throws Exception {
        RegexpQueryBuilder query = new RegexpQueryBuilder(INT_FIELD_NAME, "12");
        QueryShardContext context = createShardContext();
        QueryShardException e = expectThrows(QueryShardException.class, () -> query.toQuery(context));
        assertEquals(
            "Can only use regexp queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
            e.getMessage()
        );
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = """
            {
                "regexp": {
                  "user1": {
                    "value": "k.*y"
                  },
                  "user2": {
                    "value": "k.*y"
                  }
                }
            }""";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[regexp] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());

        String shortJson = """
            {
                "regexp": {
                  "user1": "k.*y",
                  "user2": "k.*y"
                }
            }""";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[regexp] query doesn't support multiple fields, found [user1] and [user2]", e.getMessage());
    }

    // Test that COMPLEMENT flag triggers deprecation warning
    public void testComplementFlagDeprecation() throws IOException {
        RegexpQueryBuilder query = new RegexpQueryBuilder("field", "a~bc");
        query.flags(RegexpFlag.COMPLEMENT);
        QueryShardContext context = createShardContext();
        Query luceneQuery = query.toQuery(context);
        assertNotNull(luceneQuery);
        assertThat(luceneQuery, instanceOf(RegexpQuery.class));
        assertWarnings(
            "The complement operator (~) for arbitrary patterns in regexp queries is deprecated "
                + "and will be removed in a future version. Consider rewriting your query to use character class negation [^...] or other query types."
        );
    }

    // Separate test for COMPLEMENT flag Cacheability
    public void testComplementFlagCacheability() throws IOException {
        RegexpQueryBuilder queryBuilder = new RegexpQueryBuilder("field", "pattern");
        queryBuilder.flags(RegexpFlag.COMPLEMENT);
        QueryShardContext context = createShardContext();
        QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertTrue("query should be cacheable: " + queryBuilder, context.isCacheable());
        assertWarnings(
            "The complement operator (~) for arbitrary patterns in regexp queries is deprecated "
                + "and will be removed in a future version. Consider rewriting your query to use character class negation [^...] or other query types."
        );
    }
}
