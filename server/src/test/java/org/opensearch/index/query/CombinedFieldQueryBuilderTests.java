/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.search.CombinedFieldQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.BytesRefs;
import org.opensearch.index.search.MatchQuery;
import org.opensearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.instanceOf;

public class CombinedFieldQueryBuilderTests extends AbstractQueryTestCase<CombinedFieldQueryBuilder> {
    private static final String MISSING_WILDCARD_FIELD_NAME = "missing_*";
    private static final String MISSING_FIELD_NAME = "missing";

    @Override
    protected CombinedFieldQueryBuilder doCreateTestQueryBuilder() {
        final Object value = getRandomQueryText();
        final CombinedFieldQueryBuilder query = new CombinedFieldQueryBuilder(value);
        if (randomBoolean()) {
            query.analyzer(randomAnalyzer());
        }
        if (randomBoolean()) {
            query.zeroTermsQuery(randomFrom(MatchQuery.ZeroTermsQuery.NONE, MatchQuery.ZeroTermsQuery.ALL));
        }
        int numFields = randomInt(10);
        for (int i = 0; i < numFields; i++) {
            String fieldName = randomFrom(
                TEXT_FIELD_NAME,
                TEXT_ALIAS_FIELD_NAME,
                INT_FIELD_NAME,
                DOUBLE_FIELD_NAME,
                BOOLEAN_FIELD_NAME,
                DATE_FIELD_NAME,
                MISSING_FIELD_NAME,
                MISSING_WILDCARD_FIELD_NAME
            );
            if (randomBoolean()) {
                query.field(fieldName);
            } else {
                // field with random weight
                query.field(fieldName, 1f + randomFloat() * 10);
            }
        }
        return query;
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new CombinedFieldQueryBuilder((Object) null));
    }

    @Override
    protected void doAssertLuceneQuery(CombinedFieldQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(
            query,
            anyOf(
                Arrays.asList(instanceOf(CombinedFieldQuery.class), instanceOf(MatchAllDocsQuery.class), instanceOf(MatchNoDocsQuery.class))
            )
        );
        if (query instanceof CombinedFieldQuery) {
            List<String> textFields = queryBuilder.fields().keySet().stream().filter(this::isTextField).collect(Collectors.toList());

            CombinedFieldQuery q = (CombinedFieldQuery) query;
            Map<String, Term> termsPerField = new HashMap<>();
            for (Term term : q.getTerms()) {
                termsPerField.put(term.field(), term);
            }

            assertEquals("Each text field should have terms", textFields.size(), termsPerField.size());
            String queryToString = query.toString();
            for (Map.Entry<String, Float> fieldWeight : queryBuilder.fields().entrySet()) {
                if (isTextField(fieldWeight.getKey()) == false) {
                    continue;
                }
                if (context.fieldMapper(fieldWeight.getKey()) != null) {
                    if (fieldWeight.getValue() != 1f) {
                        assertTrue(
                            "CombinedFieldQuery should contain " + fieldWeight.getKey() + " with weight",
                            queryToString.contains(fieldWeight.getKey() + "^" + fieldWeight.getValue())
                        );
                    } else {
                        assertTrue(
                            "CombinedFieldQuery should contain " + fieldWeight.getKey() + "without weight",
                            queryToString.contains(fieldWeight.getKey() + " ") || queryToString.contains(fieldWeight.getKey() + ")")
                        );
                    }
                }
            }
        }
    }

    public void testFieldWithoutWeight() throws IOException {
        CombinedFieldQueryBuilder queryBuilder = new CombinedFieldQueryBuilder("text").field(TEXT_FIELD_NAME);

        Query query = queryBuilder.toQuery(createShardContext());
        CombinedFieldQuery expected = new CombinedFieldQuery.Builder().addField(TEXT_FIELD_NAME)
            .addTerm(BytesRefs.toBytesRef("text"))
            .build();

        assertEquals(expected, query);
    }

    public void testFieldWithWeight() throws IOException {
        CombinedFieldQueryBuilder queryBuilder = new CombinedFieldQueryBuilder("text").field(TEXT_FIELD_NAME, 5.0f);

        Query query = queryBuilder.toQuery(createShardContext());
        CombinedFieldQuery expected = new CombinedFieldQuery.Builder().addField(TEXT_FIELD_NAME, 5.0f)
            .addTerm(BytesRefs.toBytesRef("text"))
            .build();

        assertEquals(expected, query);
    }

    public void testMultipleTerms() throws IOException {
        String queryString = "multi-word query string";
        CombinedFieldQueryBuilder queryBuilder = new CombinedFieldQueryBuilder(queryString).field(TEXT_FIELD_NAME);

        Query query = queryBuilder.toQuery(createShardContext());
        CombinedFieldQuery expected = new CombinedFieldQuery.Builder().addField(TEXT_FIELD_NAME)
            .addTerm(BytesRefs.toBytesRef("multi"))
            .addTerm(BytesRefs.toBytesRef("word"))
            .addTerm(BytesRefs.toBytesRef("query"))
            .addTerm(BytesRefs.toBytesRef("string"))
            .build();

        assertEquals(expected, query);
    }

    public void testCustomAnalyzer() throws IOException {
        String queryString = "multi-word query string";
        CombinedFieldQueryBuilder queryBuilder = new CombinedFieldQueryBuilder(queryString).field(TEXT_FIELD_NAME).analyzer("keyword");

        Query query = queryBuilder.toQuery(createShardContext());
        CombinedFieldQuery expected = new CombinedFieldQuery.Builder().addField(TEXT_FIELD_NAME)
            .addTerm(BytesRefs.toBytesRef(queryString))
            .build();

        assertEquals(expected, query);
    }

    public void testNoTerms() throws IOException {
        CombinedFieldQueryBuilder queryBuilder = new CombinedFieldQueryBuilder("the").field(TEXT_FIELD_NAME).analyzer("stop");

        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        queryBuilder = new CombinedFieldQueryBuilder("the").field(TEXT_FIELD_NAME)
            .analyzer("stop")
            .zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);

        query = queryBuilder.toQuery(createShardContext());
        assertThat(query, instanceOf(MatchAllDocsQuery.class));
    }

    public void testNoTextQueryFields() throws IOException {
        CombinedFieldQueryBuilder queryBuilder = new CombinedFieldQueryBuilder("text").field(MISSING_FIELD_NAME)
            .field(INT_FIELD_NAME)
            .field(INT_RANGE_FIELD_NAME)
            .field(OBJECT_FIELD_NAME)
            .zeroTermsQuery(MatchQuery.ZeroTermsQuery.ALL);

        // Even if zeroTermsQuery is ALL, we'll match nothing if there are no valid fields
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "  \"combined_field\" : {\n"
            + "    \"query\" : \"quick brown fox\",\n"
            + "    \"fields\" : [\n"
            + "      \"field1^1.0\",\n"
            + "      \"field2^5.0\"\n"
            + "    ],\n"
            + "    \"analyzer\" : \"keyword\",\n"
            + "    \"zero_terms_query\" : \"NONE\",\n"
            + "    \"boost\" : 0.5,\n"
            + "    \"_name\" : \"named_query\""
            + "  }\n"
            + "}";

        CombinedFieldQueryBuilder parsed = (CombinedFieldQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertEquals("quick brown fox", parsed.value());
        assertEquals(Map.of("field1", 1f, "field2", 5f), parsed.fields());
        assertEquals("keyword", parsed.analyzer());
        assertEquals(MatchQuery.ZeroTermsQuery.NONE, parsed.zeroTermsQuery());
    }
}
