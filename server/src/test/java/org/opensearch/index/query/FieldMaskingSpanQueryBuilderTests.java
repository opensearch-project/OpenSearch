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
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.queries.spans.FieldMaskingSpanQuery;
import org.opensearch.core.common.ParsingException;
import org.opensearch.test.AbstractQueryTestCase;

import java.io.IOException;

import static org.opensearch.index.query.FieldMaskingSpanQueryBuilder.SPAN_FIELD_MASKING_FIELD;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;

public class FieldMaskingSpanQueryBuilderTests extends AbstractQueryTestCase<FieldMaskingSpanQueryBuilder> {
    @Override
    protected FieldMaskingSpanQueryBuilder doCreateTestQueryBuilder() {
        String fieldName;
        if (randomBoolean()) {
            fieldName = randomFrom(MAPPED_FIELD_NAMES);
        } else {
            fieldName = randomAlphaOfLengthBetween(1, 10);
        }
        SpanTermQueryBuilder innerQuery = new SpanTermQueryBuilderTests().createTestQueryBuilder();
        innerQuery.boost(1f);
        return new FieldMaskingSpanQueryBuilder(innerQuery, fieldName);
    }

    @Override
    protected void doAssertLuceneQuery(FieldMaskingSpanQueryBuilder queryBuilder, Query query, QueryShardContext context)
        throws IOException {
        String fieldInQuery = expectedFieldName(queryBuilder.fieldName());
        assertThat(query, instanceOf(FieldMaskingSpanQuery.class));
        FieldMaskingSpanQuery fieldMaskingSpanQuery = (FieldMaskingSpanQuery) query;
        assertThat(fieldMaskingSpanQuery.getField(), equalTo(fieldInQuery));
        Query sub = queryBuilder.innerQuery().toQuery(context);
        assertThat(fieldMaskingSpanQuery.getMaskedQuery(), equalTo(sub));
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new FieldMaskingSpanQueryBuilder(null, "maskedField"));
        SpanQueryBuilder span = new SpanTermQueryBuilder("name", "value");
        expectThrows(IllegalArgumentException.class, () -> new FieldMaskingSpanQueryBuilder(span, null));
        expectThrows(IllegalArgumentException.class, () -> new FieldMaskingSpanQueryBuilder(span, ""));
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "  \""
            + SPAN_FIELD_MASKING_FIELD.getPreferredName()
            + "\" : {\n"
            + "    \"query\" : {\n"
            + "      \"span_term\" : {\n"
            + "        \"value\" : {\n"
            + "          \"value\" : 0.5,\n"
            + "          \"boost\" : 0.23\n"
            + "        }\n"
            + "      }\n"
            + "    },\n"
            + "    \"field\" : \"mapped_geo_shape\",\n"
            + "    \"boost\" : 42.0,\n"
            + "    \"_name\" : \"KPI\"\n"
            + "  }\n"
            + "}";
        Exception exception = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(
            exception.getMessage(),
            equalTo(
                SPAN_FIELD_MASKING_FIELD.getPreferredName() + " [query] as a nested span clause can't have non-default boost value [0.23]"
            )
        );
    }

    public void testJsonSpanTermWithBoost() throws IOException {
        String json = "{\n"
            + "  \"span_field_masking\" : {\n"
            + "    \"query\" : {\n"
            + "      \"span_term\" : {\n"
            + "        \"value\" : {\n"
            + "          \"value\" : \"term\"\n"
            + "        }\n"
            + "      }\n"
            + "    },\n"
            + "    \"field\" : \"mapped_geo_shape\",\n"
            + "    \"boost\" : 42.0,\n"
            + "    \"_name\" : \"KPI\"\n"
            + "  }\n"
            + "}";
        Query query = parseQuery(json).toQuery(createShardContext());
        assertEquals(
            new BoostQuery(new FieldMaskingSpanQuery(new SpanTermQuery(new Term("value", "term")), "mapped_geo_shape"), 42f),
            query
        );
    }

    public void testDeprecatedName() throws IOException {
        String json = "{\n"
            + "  \"field_masking_span\" : {\n"
            + "    \"query\" : {\n"
            + "      \"span_term\" : {\n"
            + "        \"value\" : {\n"
            + "          \"value\" : \"foo\"\n"
            + "        }\n"
            + "      }\n"
            + "    },\n"
            + "    \"field\" : \"mapped_geo_shape\",\n"
            + "    \"boost\" : 42.0,\n"
            + "    \"_name\" : \"KPI\"\n"
            + "  }\n"
            + "}";
        FieldMaskingSpanQueryBuilder parsed = (FieldMaskingSpanQueryBuilder) parseQuery(json);
        assertWarnings(
            "Deprecated field [field_masking_span] used, expected [" + SPAN_FIELD_MASKING_FIELD.getPreferredName() + "] instead"
        );
    }
}
