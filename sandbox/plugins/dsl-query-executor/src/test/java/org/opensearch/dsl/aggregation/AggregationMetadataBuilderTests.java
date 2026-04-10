/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregationMetadataBuilderTests extends OpenSearchTestCase {

    public void testContainsAggregateFieldReturnsTrueForExistingField() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addAggregateCall(dummySumCall("total_revenue"), "total_revenue");

        assertTrue(builder.containsAggregateField("total_revenue"));
    }

    public void testContainsAggregateFieldReturnsFalseForMissingField() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addAggregateCall(dummySumCall("total_revenue"), "total_revenue");

        assertFalse(builder.containsAggregateField("nonexistent"));
    }

    public void testAggNameMapping() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addAggNameMapping("by_brand", "brand");
        builder.addAggNameMapping("by_region", "region");

        Map<String, String> target = new LinkedHashMap<>();
        builder.collectAggNameMappings(target);

        assertEquals("brand", target.get("by_brand"));
        assertEquals("region", target.get("by_region"));
    }

    public void testAggNameMappingForFilterBucket() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addAggNameMapping("expensive_only", "__filter__expensive_only");

        Map<String, String> target = new LinkedHashMap<>();
        builder.collectAggNameMappings(target);

        assertEquals("__filter__expensive_only", target.get("expensive_only"));
    }

    public void testAggNameMappingForMultiFilterBucket() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addAggNameMapping("my_filters", "__filter__my_filters/errors");

        Map<String, String> target = new LinkedHashMap<>();
        builder.collectAggNameMappings(target);

        assertEquals("__filter__my_filters/errors", target.get("my_filters"));
    }

    public void testCollectFromMultipleBuilders() {
        AggregationMetadataBuilder builder1 = new AggregationMetadataBuilder();
        builder1.addAggNameMapping("by_region", "region");

        AggregationMetadataBuilder builder2 = new AggregationMetadataBuilder();
        builder2.addAggNameMapping("by_brand", "brand");

        Map<String, String> target = new LinkedHashMap<>();
        builder1.collectAggNameMappings(target);
        builder2.collectAggNameMappings(target);

        assertEquals("region", target.get("by_region"));
        assertEquals("brand", target.get("by_brand"));
    }

    public void testHasAggregateCallsWithMetric() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addAggregateCall(dummySumCall("total"), "total");

        assertTrue(builder.hasAggregateCalls());
    }

    public void testHasAggregateCallsWithImplicitCount() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.requestImplicitCount();

        assertTrue(builder.hasAggregateCalls());
    }

    public void testHasAggregateCallsEmpty() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();

        assertFalse(builder.hasAggregateCalls());
    }

    public static AggregateCall dummySumCall(String name) {
        return AggregateCall.create(
            SqlStdOperatorTable.SUM, false, false, false,
            ImmutableList.of(), List.of(0), -1, null,
            RelCollations.EMPTY, new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
                .createSqlType(SqlTypeName.INTEGER), name
        );
    }
}
