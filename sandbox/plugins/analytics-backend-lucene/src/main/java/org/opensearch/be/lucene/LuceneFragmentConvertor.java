/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.dag.BackendPlanAdapter;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.ArrowBatchSourcePlan;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.WireFormat;
import org.opensearch.be.lucene.serializers.AbstractQuerySerializer;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;

import io.substrait.proto.NamedStruct;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.Type;

/**
 * Converts a {@link LuceneFragmentPlanner} result into a typed Lucene shard payload.
 * Metadata counts stay on Lucene; supported doc-values fragments include an
 * {@link ArrowBatchSourcePlan} compiled for DataFusion. Unsupported fragments remain
 * available only as planner alternatives and are not selected for Lucene execution.
 *
 * <p>Filters reuse {@link QuerySerializerRegistry}, so delegated and driver execution use
 * the same {@link QueryBuilder} conversion.
 *
 * @opensearch.internal
 */
final class LuceneFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(LuceneFragmentConvertor.class);

    private final Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers;
    private final AnalyticsSearchBackendPlugin arrowBatchSourceBackend;

    LuceneFragmentConvertor(
        Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers,
        AnalyticsSearchBackendPlugin arrowBatchSourceBackend
    ) {
        this.leafSerializers = leafSerializers;
        this.arrowBatchSourceBackend = arrowBatchSourceBackend;
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        LuceneFragmentPlanner.Shape shape = LuceneFragmentPlanner.classify(fragment);
        if (shape instanceof LuceneFragmentPlanner.ArrowSourceShape arrowSource) {
            return convertArrowSourceShape(arrowSource, false);
        }

        QueryBuilder filterQuery = toQueryBuilder(shape.filter());
        byte[] bytes = LuceneFragmentWirePlan.create(shape.outputNames(), filterQuery, null).toBytes();
        LOGGER.debug(
            "[lucene-count] convertFragment outputNames={} filterQuery={} bytes={}",
            shape.outputNames(),
            filterQuery,
            bytes.length
        );
        return bytes;
    }

    private byte[] convertArrowSourceShape(LuceneFragmentPlanner.ArrowSourceShape shape, boolean partialAggregate) {
        AnalyticsSearchBackendPlugin backend = requireArrowBatchSourceBackend();
        RelNode adapted = BackendPlanAdapter.adaptFragment(shape.rebasedFragment(), backend.getCapabilityProvider());
        FragmentConvertor convertor = backend.getFragmentConvertor();
        byte[] planBytes;
        if (partialAggregate) {
            if (adapted instanceof Aggregate == false) {
                throw new IllegalArgumentException("partial Arrow source plan must be rooted at an Aggregate");
            }
            Aggregate aggregate = (Aggregate) adapted;
            planBytes = convertor.attachPartialAggOnTop(aggregate, convertor.convertFragment(aggregate.getInput()));
        } else {
            planBytes = convertor.convertFragment(adapted);
        }
        QueryBuilder filterQuery = toQueryBuilder(shape.filter());
        ArrowBatchSourcePlan sourcePlan = new ArrowBatchSourcePlan(shape.inputId(), planBytes, shape.inputColumns());
        byte[] bytes = LuceneFragmentWirePlan.create(shape.outputNames(), filterQuery, sourcePlan).toBytes();
        LOGGER.debug(
            "[lucene-arrow-source] inputColumns={} outputNames={} plan={}B filter={} bytes={}",
            shape.inputColumns(),
            shape.outputNames(),
            planBytes.length,
            filterQuery,
            bytes.length
        );
        return bytes;
    }

    private QueryBuilder toQueryBuilder(Filter filter) {
        return filter == null ? null : toQueryBuilder(filter.getCondition(), findFieldStorage(filter));
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        if (partialAggFragment instanceof Aggregate == false) {
            throw new IllegalStateException(
                "Lucene attachPartialAggOnTop expected an Aggregate fragment, got " + partialAggFragment.getClass().getSimpleName()
            );
        }
        LuceneFragmentPlanner.Shape shape = LuceneFragmentPlanner.classify(partialAggFragment);
        if (shape instanceof LuceneFragmentPlanner.ArrowSourceShape arrowSource) {
            return convertArrowSourceShape(arrowSource, true);
        }

        byte[] bytes = LuceneFragmentWirePlan.fromBytes(innerBytes).withOutputNames(shape.outputNames()).toBytes();
        LOGGER.debug("[lucene-count] attachPartialAggOnTop outputNames={} bytes={}", shape.outputNames(), bytes.length);
        return bytes;
    }

    @Override
    public byte[] attachFragmentOnTop(RelNode fragment, byte[] innerBytes) {
        LuceneFragmentWirePlan inner = LuceneFragmentWirePlan.fromBytes(innerBytes);
        ArrowBatchSourcePlan innerPlan = inner.arrowSourcePlan();
        if (innerPlan == null) {
            throw new UnsupportedOperationException("Cannot attach a fragment to the Lucene count wire format");
        }
        AnalyticsSearchBackendPlugin backend = requireArrowBatchSourceBackend();
        RelNode adapted = BackendPlanAdapter.adaptFragment(fragment, backend.getCapabilityProvider());
        byte[] planBytes = backend.getFragmentConvertor().attachFragmentOnTop(adapted, innerPlan.planBytes());
        ArrowBatchSourcePlan plan = new ArrowBatchSourcePlan(innerPlan.inputId(), planBytes, innerPlan.inputColumns());
        return inner.withArrowSourcePlan(plan, LuceneFragmentPlanner.resultNames(fragment)).toBytes();
    }

    private AnalyticsSearchBackendPlugin requireArrowBatchSourceBackend() {
        if (arrowBatchSourceBackend == null) {
            throw new IllegalStateException("No backend supports Arrow batch source execution");
        }
        return arrowBatchSourceBackend;
    }

    @Override
    public WireFormat wireFormat() {
        // convertFragment emits a typed OpenSearch wire payload, not Substrait. The orchestrator queries this so it
        // knows to emit a separate schema-only stub via convertSchemaOnlyRead for the
        // coordinator's reduce-sink partition registration.
        return WireFormat.OPAQUE;
    }

    /**
     * Substrait stub describing the count fragment's output partition: one
     * {@code Plan{Read{named_table; base_schema}}} carrying the partition's named-table id
     * and column types. Mirrors {@code DataFusionFragmentConvertor.convertSchemaOnlyRead} —
     * same proto shape, decoded by the same Rust {@code derive_schema_from_partial_plan} on
     * the coordinator.
     *
     * <p>In production (selector with default {@code prefer_metadata_driver=true}) the only
     * Lucene plans reaching this method are the Aggregate-rooted count fast path, where the
     * stub describes a single {@code I64 NOT NULL} column per aggregate call. Tests that pin
     * {@code prefer=false} keep both alternatives — the Lucene plan there can be Filter-rooted
     * over the upstream scan rowType, which is why {@link #toSubstraitType} maps a few extra
     * primitives. Those bytes are never dispatched (the data node picks the peer alternative);
     * the mapping exists so the test path doesn't blow up at conversion.
     */
    @Override
    public byte[] convertSchemaOnlyRead(int childStageId, RelDataType rowType) {
        // Struct-level nullability stays REQUIRED (the row itself is always present); per-field
        // nullability is encoded inside each Type via toSubstraitType. Declared per-field
        // nullability MUST match what LuceneSearchExecEngine.buildSchema produces — Lucene's
        // count emission uses nullable Int64, so the stub's columns must say NULLABLE too. A
        // mismatch here used to silently hang at the partition stream (Rust registers a
        // NOT-NULL partition, runtime batches arrive nullable, drain stalls).
        Type.Struct.Builder structBuilder = Type.Struct.newBuilder().setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        NamedStruct.Builder namedStructBuilder = NamedStruct.newBuilder();
        for (RelDataTypeField field : rowType.getFieldList()) {
            namedStructBuilder.addNames(field.getName());
            structBuilder.addTypes(toSubstraitType(field.getType()));
        }
        namedStructBuilder.setStruct(structBuilder.build());

        ReadRel readRel = ReadRel.newBuilder()
            .setNamedTable(ReadRel.NamedTable.newBuilder().addNames("input-" + childStageId).build())
            .setBaseSchema(namedStructBuilder.build())
            .build();
        Rel inputRel = Rel.newBuilder().setRead(readRel).build();
        PlanRel planRel = PlanRel.newBuilder()
            .setRoot(RelRoot.newBuilder().setInput(inputRel).addAllNames(rowType.getFieldNames()).build())
            .build();

        byte[] bytes = Plan.newBuilder().addRelations(planRel).build().toByteArray();
        LOGGER.debug(
            "[lucene-count] convertSchemaOnlyRead stage={} fields={} bytes={}",
            childStageId,
            rowType.getFieldNames(),
            bytes.length
        );
        return bytes;
    }

    /**
     * Minimal Calcite→Substrait type mapper for the schema-only Read. Covers the count
     * fast path (BIGINT) plus the few primitives a non-driver Lucene plan's row type can
     * carry (text/keyword → string, numerics, boolean). The result is only used for
     * coordinator-side partition registration; the bytes never round-trip back to a
     * Calcite type.
     *
     * <p><b>Nullability:</b> Calcite's COUNT aggregate types as BIGINT NOT NULL, but Lucene's
     * runtime emits a nullable Int64 column ({@code LuceneSearchExecEngine.buildSchema}
     * builds {@code FieldType(true, Int(64,true), null)} — the leading {@code true} is
     * nullable). The Substrait stub MUST reflect the producer's actual runtime schema, not
     * the Calcite logical type, otherwise the Rust-side partition stream registers as
     * NOT-NULL and silently stalls when nullable batches arrive. Force nullable for now;
     * when the driver supports more shapes, this will need a per-column source-of-truth.
     *
     * <p>TODO: when Lucene-driver shapes beyond COUNT land (group-by-count keys), wire in a
     * proper Calcite→Substrait converter so the stub describes real producer schemas.
     */
    private static Type toSubstraitType(RelDataType type) {
        // Always nullable to match LuceneSearchExecEngine.buildSchema's output. See class doc.
        Type.Nullability n = Type.Nullability.NULLABILITY_NULLABLE;
        return switch (type.getSqlTypeName()) {
            case BIGINT -> Type.newBuilder().setI64(Type.I64.newBuilder().setNullability(n)).build();
            case INTEGER -> Type.newBuilder().setI32(Type.I32.newBuilder().setNullability(n)).build();
            case SMALLINT -> Type.newBuilder().setI16(Type.I16.newBuilder().setNullability(n)).build();
            case TINYINT -> Type.newBuilder().setI8(Type.I8.newBuilder().setNullability(n)).build();
            case BOOLEAN -> Type.newBuilder().setBool(Type.Boolean.newBuilder().setNullability(n)).build();
            case DOUBLE -> Type.newBuilder().setFp64(Type.FP64.newBuilder().setNullability(n)).build();
            case FLOAT, REAL -> Type.newBuilder().setFp32(Type.FP32.newBuilder().setNullability(n)).build();
            case VARCHAR, CHAR -> Type.newBuilder().setString(Type.String.newBuilder().setNullability(n)).build();
            case DATE, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> Type.newBuilder()
                .setPrecisionTimestamp(Type.PrecisionTimestamp.newBuilder().setPrecision(3).setNullability(n))
                .build();
            default -> throw new IllegalStateException(
                "Lucene convertSchemaOnlyRead: unmapped Calcite type " + type.getSqlTypeName() + " for field of type " + type
            );
        };
    }

    /**
     * Returns the field-storage info for a filter's child operator. When the filter is a
     * native {@link OpenSearchFilter} this is just its own {@code getOutputFieldStorage()};
     * for a plain {@code LogicalFilter} produced by {@code strip()}, walk the input chain to
     * the nearest {@link OpenSearchRelNode} (the TableScan) and use its storage. Per-leaf
     * serializers consult this list to resolve column references back to their backing fields.
     */
    private static List<FieldStorageInfo> findFieldStorage(Filter filter) {
        if (filter instanceof OpenSearchFilter osf) {
            return osf.getOutputFieldStorage();
        }
        RelNode current = filter.getInput();
        while (current != null) {
            if (current instanceof OpenSearchRelNode osNode) {
                return osNode.getOutputFieldStorage();
            }
            if (current.getInputs().isEmpty()) break;
            current = current.getInputs().getFirst();
        }
        // Every Lucene-driver fragment has an OpenSearchTableScan ancestor by construction
        // (the table-scan rule wraps it before forking). If we got here, FragmentConversionDriver
        // produced an unexpected shape — fail loud so the planner bug is visible at conversion
        // time, not later when a serializer NPEs on missing field storage.
        throw new IllegalStateException("Lucene-driver filter has no OpenSearchRelNode ancestor: " + filter);
    }

    /**
     * Recursively converts a filter condition RexNode to a {@link QueryBuilder}. Mirrors
     * {@link LuceneSubtreeConvertor#toQueryBuilder} — same boolean structure handling
     * (AND→MUST, OR→SHOULD, NOT→MUST_NOT), same per-leaf serializer lookup. The duplication
     * is intentional: the delegation flow operates on a {@code DelegatedSubtreeConvertor}
     * SPI typed for serialized-bytes output, while the driver flow operates on
     * {@link FragmentConvertor} typed for whole-fragment serialization. Sharing the leaf
     * logic via a shared helper would be a follow-up cleanup.
     */
    private QueryBuilder toQueryBuilder(RexNode node, List<FieldStorageInfo> fieldStorage) {
        if (node instanceof AnnotatedPredicate ap) {
            node = ap.unwrap();
        }
        if (node instanceof RexCall call) {
            switch (call.getKind()) {
                case AND: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.must(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case OR: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    for (RexNode child : call.getOperands()) {
                        b.should(toQueryBuilder(child, fieldStorage));
                    }
                    return b;
                }
                case NOT: {
                    BoolQueryBuilder b = new BoolQueryBuilder();
                    b.mustNot(toQueryBuilder(call.getOperands().get(0), fieldStorage));
                    return b;
                }
                default:
                    return leafToQueryBuilder(call, fieldStorage);
            }
        }
        throw new IllegalStateException("Unexpected RexNode in Lucene-driver filter condition: " + node);
    }

    private QueryBuilder leafToQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        ScalarFunction fn = ScalarFunction.fromSqlOperatorWithFallback(call.getOperator());
        if (fn == null) {
            throw new IllegalStateException("Unrecognized operator in Lucene-driver filter: " + call.getOperator());
        }
        DelegatedPredicateSerializer serializer = leafSerializers.get(fn);
        if (serializer == null) {
            throw new IllegalStateException("No Lucene serializer for [" + fn + "] in driver-mode filter");
        }
        return ((AbstractQuerySerializer) serializer).buildQueryBuilder(call, fieldStorage);
    }
}
