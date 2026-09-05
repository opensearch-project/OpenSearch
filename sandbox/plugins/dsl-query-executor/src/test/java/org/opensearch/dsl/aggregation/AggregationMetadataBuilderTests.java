/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.aggregation;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

public class AggregationMetadataBuilderTests extends OpenSearchTestCase {

    private final ConversionContext ctx = TestUtils.createContext();

    public void testResolvesFieldGroupingToCorrectIndex() throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        // brand is the 3rd field (index 2) in TestUtils schema: name, price, brand, rating
        builder.addGrouping(new FieldGrouping(List.of("brand")));
        builder.requestImplicitCount();

        AggregationMetadata metadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertTrue(metadata.getGroupByBitSet().get(2));
        assertEquals(1, metadata.getGroupByBitSet().cardinality());
    }

    public void testResolvesMultipleFieldGroupings() throws ConversionException {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping(List.of("brand")));
        builder.addGrouping(new FieldGrouping(List.of("name")));
        builder.requestImplicitCount();

        AggregationMetadata metadata = builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());

        assertTrue(metadata.getGroupByBitSet().get(0)); // name is index 0
        assertTrue(metadata.getGroupByBitSet().get(2)); // brand is index 2
        assertEquals(2, metadata.getGroupByBitSet().cardinality());
    }

    public void testThrowsForUnknownField() {
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping(List.of("nonexistent")));
        builder.requestImplicitCount();

        expectThrows(ConversionException.class, () -> builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory()));
    }

    /**
     * A {@code SUM} over an {@code INTEGER} column must be declared {@code BIGINT} — the width
     * {@code DslTypeSystems.NANO_TIMESTAMP} derives, and therefore the width
     * {@code Aggregate.typeMatchesInferred} demands. The metric translators declare the summed
     * column's own type, so without the reconciliation in {@code build} this plan does not construct
     * at all: {@code LogicalAggregate.create} throws {@code type mismatch: aggCall type INTEGER,
     * inferred type BIGINT}. Asserted on the metadata rather than through a built plan so the failure
     * names the type, not a Calcite assertion.
     */
    public void testSumOverAnIntegerColumnIsDeclaredBigint() throws ConversionException {
        AggregationMetadata metadata = withMetric(SqlStdOperatorTable.SUM, "price", "sum_price");

        AggregateCall reconciled = metadata.getAggregateCalls().get(0);
        assertEquals(SqlTypeName.BIGINT, reconciled.getType().getSqlTypeName());
        assertTrue("nullability of the summed column must survive the widening", reconciled.getType().isNullable());
    }

    /**
     * The other half of the same contract, and the boundary of it: {@code MIN} and {@code MAX} keep the
     * column's own type, while {@code AVG} is declared {@code DOUBLE}.
     */
    public void testMinAndMaxStayIntegerWhileAvgIsDouble() throws ConversionException {
        for (SqlAggFunction fn : List.of(SqlStdOperatorTable.MIN, SqlStdOperatorTable.MAX)) {
            AggregationMetadata metadata = withMetric(fn, "price", fn.getName() + "_price");

            assertEquals(
                fn.getName() + " must keep the aggregated column's own type",
                SqlTypeName.INTEGER,
                metadata.getAggregateCalls().get(0).getType().getSqlTypeName()
            );
        }

        AggregationMetadata avg = withMetric(SqlStdOperatorTable.AVG, "price", "avg_price");
        assertEquals(
            "AVG must be declared DOUBLE, or the CAST it emits undoes the SUM widening and the mean is truncated",
            SqlTypeName.DOUBLE,
            avg.getAggregateCalls().get(0).getType().getSqlTypeName()
        );
    }

    /**
     * One grouped aggregation carrying one metric, built exactly as the metric translators build it:
     * the call declares the <em>input column's</em> type (see
     * {@code AbstractMetricTranslator.toAggregateCall}), which is the input the reconciliation exists
     * to correct.
     */
    private AggregationMetadata withMetric(SqlAggFunction fn, String field, String name) throws ConversionException {
        RelDataTypeField column = ctx.getRowType().getField(field, false, false);
        AggregationMetadataBuilder builder = new AggregationMetadataBuilder();
        builder.addGrouping(new FieldGrouping(List.of("brand")));
        builder.addAggregateCall(
            AggregateCall.create(fn, false, false, false, List.of(column.getIndex()), -1, RelCollations.EMPTY, column.getType(), name),
            name
        );
        return builder.build(ctx.getRowType(), ctx.getCluster().getTypeFactory());
    }
}
