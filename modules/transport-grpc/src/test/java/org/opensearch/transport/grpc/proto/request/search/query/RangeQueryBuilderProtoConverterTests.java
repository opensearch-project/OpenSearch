/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.protobufs.DateRangeQuery;
import org.opensearch.protobufs.DateRangeQueryAllOfFrom;
import org.opensearch.protobufs.DateRangeQueryAllOfTo;
import org.opensearch.protobufs.QueryContainer;
import org.opensearch.protobufs.RangeQuery;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class RangeQueryBuilderProtoConverterTests extends OpenSearchTestCase {

    private RangeQueryBuilderProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new RangeQueryBuilderProtoConverter();
    }

    public void testGetHandledQueryCase() {
        assertEquals(QueryContainer.QueryContainerCase.RANGE, converter.getHandledQueryCase());
    }

    public void testFromProtoWithValidRangeQuery() {
        DateRangeQueryAllOfFrom fromObj = DateRangeQueryAllOfFrom.newBuilder().setString("2023-01-01").build();

        DateRangeQueryAllOfTo toObj = DateRangeQueryAllOfTo.newBuilder().setString("2023-12-31").build();

        DateRangeQuery dateRangeQuery = DateRangeQuery.newBuilder().setField("date_field").setFrom(fromObj).setTo(toObj).build();

        RangeQuery rangeQuery = RangeQuery.newBuilder().setDateRangeQuery(dateRangeQuery).build();

        QueryContainer queryContainer = QueryContainer.newBuilder().setRange(rangeQuery).build();

        QueryBuilder result = converter.fromProto(queryContainer);

        assertThat(result, instanceOf(RangeQueryBuilder.class));
        RangeQueryBuilder rangeQueryBuilder = (RangeQueryBuilder) result;
        assertEquals("date_field", rangeQueryBuilder.fieldName());
    }

    public void testFromProtoWithNullQueryContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));

        assertThat(exception.getMessage(), containsString("QueryContainer must contain a RangeQuery"));
    }

    public void testFromProtoWithWrongQueryType() {
        QueryContainer queryContainer = QueryContainer.newBuilder()
            .setTerm(
                org.opensearch.protobufs.TermQuery.newBuilder()
                    .setField("test_field")
                    .setValue(org.opensearch.protobufs.FieldValue.newBuilder().setString("test_value").build())
                    .build()
            )
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));

        assertThat(exception.getMessage(), containsString("QueryContainer must contain a RangeQuery"));
    }

    public void testFromProtoWithUnsetQueryContainer() {

        QueryContainer queryContainer = QueryContainer.newBuilder().build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(queryContainer));

        assertThat(exception.getMessage(), containsString("QueryContainer must contain a RangeQuery"));
    }
}
