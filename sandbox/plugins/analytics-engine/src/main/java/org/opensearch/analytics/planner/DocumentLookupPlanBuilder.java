/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptAbstractTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.OpenSearchException;
import org.opensearch.action.support.ReadAccessPolicy;
import org.opensearch.analytics.EngineContextProvider;
import org.opensearch.core.index.Index;
import org.opensearch.index.engine.dataformat.DocumentInput;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

/** Builds the row-id-constrained logical plan used for DLS-aware document lookup. */
public final class DocumentLookupPlanBuilder {

    private final EngineContextProvider contextProvider;
    private final LogicalPlanDlsRewriter dlsRewriter;

    public DocumentLookupPlanBuilder(EngineContextProvider contextProvider, LogicalPlanDlsRewriter dlsRewriter) {
        this.contextProvider = contextProvider;
        this.dlsRewriter = dlsRewriter;
    }

    /**
     * Builds {@code Filter(__row_id__ = rowId, Filter(dls, Scan(index)))}. The caller must only
     * invoke this for a restricted policy; missing policy coverage fails closed in the DLS rewriter.
     */
    public RelNode build(Index index, long rowId, ReadAccessPolicy policy) {
        if (policy.hasRestrictions() == false) {
            throw new IllegalArgumentException("A restricted read-access policy is required for secured document lookup");
        }

        RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
        RelOptCluster cluster = RelOptCluster.create(new HepPlanner(new HepProgramBuilder().build()), new RexBuilder(typeFactory));
        SchemaPlus schema = contextProvider.getContext().schema();
        Table schemaTable = schema.getTable(index.getName());
        if (schemaTable == null) {
            throw new OpenSearchException("No analytics table exists for index [" + index.getName() + "]");
        }

        RelDataType rowType = withLookupMetadata(schemaTable.getRowType(typeFactory), typeFactory);
        LogicalTableScan scan = LogicalTableScan.create(cluster, new LookupTable(index.getName(), rowType), List.of());
        RelNode dlsPlan = dlsRewriter.rewrite(scan, policy, Map.of(index.getName(), List.of(index.getName())));

        int rowIdField = dlsPlan.getRowType().getFieldNames().indexOf(DocumentInput.ROW_ID_FIELD);
        if (rowIdField < 0) {
            throw new OpenSearchException("Document lookup plan does not expose [" + DocumentInput.ROW_ID_FIELD + "]");
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode rowIdPredicate = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(dlsPlan, rowIdField),
            rexBuilder.makeExactLiteral(BigDecimal.valueOf(rowId), typeFactory.createSqlType(SqlTypeName.BIGINT))
        );
        return LogicalFilter.create(dlsPlan, rowIdPredicate);
    }

    private static RelDataType withLookupMetadata(RelDataType mappedType, RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        mappedType.getFieldList().forEach(field -> builder.add(field.getName(), field.getType()));
        addIfMissing(builder, mappedType, DocumentInput.ROW_ID_FIELD, SqlTypeName.BIGINT, typeFactory);
        addIfMissing(builder, mappedType, "_seq_no", SqlTypeName.BIGINT, typeFactory);
        addIfMissing(builder, mappedType, "_primary_term", SqlTypeName.BIGINT, typeFactory);
        addIfMissing(builder, mappedType, "_version", SqlTypeName.BIGINT, typeFactory);
        addIfMissing(builder, mappedType, "_id", SqlTypeName.VARBINARY, typeFactory);
        return builder.build();
    }

    private static void addIfMissing(
        RelDataTypeFactory.Builder builder,
        RelDataType rowType,
        String field,
        SqlTypeName type,
        RelDataTypeFactory typeFactory
    ) {
        if (rowType.getField(field, true, false) == null) {
            builder.add(field, typeFactory.createSqlType(type));
        }
    }

    private static final class LookupTable extends RelOptAbstractTable {
        private LookupTable(String indexName, RelDataType rowType) {
            super(null, indexName, rowType);
        }
    }
}
