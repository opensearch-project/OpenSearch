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

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.opensearch.core.common.ParsingException;
import org.opensearch.lucene.queries.ExtendedCommonTermsQuery;
import org.opensearch.test.AbstractQueryTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.query.QueryBuilders.commonTermsQuery;
import static org.opensearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class CommonTermsQueryBuilderTests extends AbstractQueryTestCase<CommonTermsQueryBuilder> {

    @Override
    protected CommonTermsQueryBuilder doCreateTestQueryBuilder() {
        int numberOfTerms = randomIntBetween(0, 10);
        StringBuilder text = new StringBuilder("");
        for (int i = 0; i < numberOfTerms; i++) {
            text.append(randomAlphaOfLengthBetween(1, 10)).append(" ");
        }

        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, randomAlphaOfLengthBetween(1, 10));
        CommonTermsQueryBuilder query = new CommonTermsQueryBuilder(fieldName, text.toString());

        if (randomBoolean()) {
            query.cutoffFrequency(randomIntBetween(1, 10));
        }

        if (randomBoolean()) {
            query.lowFreqOperator(randomFrom(Operator.values()));
        }

        // number of low frequency terms that must match
        if (randomBoolean()) {
            query.lowFreqMinimumShouldMatch("" + randomIntBetween(1, 5));
        }

        if (randomBoolean()) {
            query.highFreqOperator(randomFrom(Operator.values()));
        }

        // number of high frequency terms that must match
        if (randomBoolean()) {
            query.highFreqMinimumShouldMatch("" + randomIntBetween(1, 5));
        }

        if (randomBoolean()) {
            query.analyzer(randomAnalyzer());
        }

        return query;
    }

    @Override
    protected Map<String, CommonTermsQueryBuilder> getAlternateVersions() {
        Map<String, CommonTermsQueryBuilder> alternateVersions = new HashMap<>();
        CommonTermsQueryBuilder commonTermsQuery = new CommonTermsQueryBuilder(
            randomAlphaOfLengthBetween(1, 10),
            randomAlphaOfLengthBetween(1, 10)
        );
        String contentString = "{\n"
            + "    \"common\" : {\n"
            + "        \""
            + commonTermsQuery.fieldName()
            + "\" : \""
            + commonTermsQuery.value()
            + "\"\n"
            + "    }\n"
            + "}";
        alternateVersions.put(contentString, commonTermsQuery);
        return alternateVersions;
    }

    @Override
    protected void doAssertLuceneQuery(CommonTermsQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(ExtendedCommonTermsQuery.class));
        ExtendedCommonTermsQuery extendedCommonTermsQuery = (ExtendedCommonTermsQuery) query;

        List<Term> terms = extendedCommonTermsQuery.getTerms();
        if (!terms.isEmpty()) {
            String expectedFieldName = expectedFieldName(queryBuilder.fieldName());
            String actualFieldName = terms.iterator().next().field();
            assertThat(actualFieldName, equalTo(expectedFieldName));
        }

        assertThat(extendedCommonTermsQuery.getHighFreqMinimumNumberShouldMatchSpec(), equalTo(queryBuilder.highFreqMinimumShouldMatch()));
        assertThat(extendedCommonTermsQuery.getLowFreqMinimumNumberShouldMatchSpec(), equalTo(queryBuilder.lowFreqMinimumShouldMatch()));
    }

    @Override
    public void testUnknownField() throws IOException {
        super.testUnknownField();
        assertDeprecationWarning();
    }

    @Override
    public void testUnknownObjectException() throws IOException {
        super.testUnknownObjectException();
        assertDeprecationWarning();
    }

    @Override
    public void testFromXContent() throws IOException {
        super.testFromXContent();
        assertDeprecationWarning();
    }

    @Override
    public void testValidOutput() throws IOException {
        super.testValidOutput();
        assertDeprecationWarning();
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new CommonTermsQueryBuilder(null, "text"));
        assertEquals("field name is null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new CommonTermsQueryBuilder("", "text"));
        assertEquals("field name is null or empty", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> new CommonTermsQueryBuilder("fieldName", null));
        assertEquals("text cannot be null", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String query = "{\n"
            + "  \"common\" : {\n"
            + "    \"body\" : {\n"
            + "      \"query\" : \"nelly the elephant not as a cartoon\",\n"
            + "      \"high_freq_operator\" : \"AND\",\n"
            + "      \"low_freq_operator\" : \"OR\",\n"
            + "      \"cutoff_frequency\" : 0.001,\n"
            + "      \"minimum_should_match\" : {\n"
            + "        \"low_freq\" : \"2\",\n"
            + "        \"high_freq\" : \"3\"\n"
            + "      },\n"
            + "      \"boost\" : 42.0\n"
            + "    }\n"
            + "  }\n"
            + "}";

        CommonTermsQueryBuilder queryBuilder = (CommonTermsQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);

        assertEquals(query, 42, queryBuilder.boost, 0.00001);
        assertEquals(query, 0.001, queryBuilder.cutoffFrequency(), 0.0001);
        assertEquals(query, Operator.OR, queryBuilder.lowFreqOperator());
        assertEquals(query, Operator.AND, queryBuilder.highFreqOperator());
        assertEquals(query, "nelly the elephant not as a cartoon", queryBuilder.value());

        assertDeprecationWarning();
    }

    public void testCommonTermsQuery1() throws IOException {
        String query = copyToStringFromClasspath("/org/opensearch/index/query/commonTerms-query1.json");
        Query parsedQuery = parseQuery(query).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(ExtendedCommonTermsQuery.class));
        ExtendedCommonTermsQuery ectQuery = (ExtendedCommonTermsQuery) parsedQuery;
        assertThat(ectQuery.getHighFreqMinimumNumberShouldMatchSpec(), nullValue());
        assertThat(ectQuery.getLowFreqMinimumNumberShouldMatchSpec(), equalTo("2"));

        assertDeprecationWarning();
    }

    public void testCommonTermsQuery2() throws IOException {
        String query = copyToStringFromClasspath("/org/opensearch/index/query/commonTerms-query2.json");
        Query parsedQuery = parseQuery(query).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(ExtendedCommonTermsQuery.class));
        ExtendedCommonTermsQuery ectQuery = (ExtendedCommonTermsQuery) parsedQuery;
        assertThat(ectQuery.getHighFreqMinimumNumberShouldMatchSpec(), equalTo("50%"));
        assertThat(ectQuery.getLowFreqMinimumNumberShouldMatchSpec(), equalTo("5<20%"));

        assertDeprecationWarning();
    }

    public void testCommonTermsQuery3() throws IOException {
        String query = copyToStringFromClasspath("/org/opensearch/index/query/commonTerms-query3.json");
        Query parsedQuery = parseQuery(query).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(ExtendedCommonTermsQuery.class));
        ExtendedCommonTermsQuery ectQuery = (ExtendedCommonTermsQuery) parsedQuery;
        assertThat(ectQuery.getHighFreqMinimumNumberShouldMatchSpec(), nullValue());
        assertThat(ectQuery.getLowFreqMinimumNumberShouldMatchSpec(), equalTo("2"));

        assertDeprecationWarning();
    }

    // see #11730
    public void testCommonTermsQuery4() throws IOException {
        Query parsedQuery = parseQuery(commonTermsQuery("field", "text")).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(ExtendedCommonTermsQuery.class));

        assertDeprecationWarning();
    }

    public void testParseFailsWithMultipleFields() throws IOException {
        String json = "{\n"
            + "  \"common\" : {\n"
            + "    \"message1\" : {\n"
            + "      \"query\" : \"nelly the elephant not as a cartoon\"\n"
            + "    },\n"
            + "    \"message2\" : {\n"
            + "      \"query\" : \"nelly the elephant not as a cartoon\"\n"
            + "    }\n"
            + "  }\n"
            + "}";

        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertEquals("[common] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        String shortJson = "{\n"
            + "  \"common\" : {\n"
            + "    \"message1\" : \"nelly the elephant not as a cartoon\",\n"
            + "    \"message2\" : \"nelly the elephant not as a cartoon\"\n"
            + "  }\n"
            + "}";
        e = expectThrows(ParsingException.class, () -> parseQuery(shortJson));
        assertEquals("[common] query doesn't support multiple fields, found [message1] and [message2]", e.getMessage());

        assertDeprecationWarning();
    }

    private void assertDeprecationWarning() {
        assertWarnings("Deprecated field [common] used, replaced by [" + CommonTermsQueryBuilder.COMMON_TERMS_QUERY_DEPRECATION_MSG + "]");
    }
}
