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
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FragmentConvertor;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.WireFormat;
import org.opensearch.be.lucene.serializers.AbstractQuerySerializer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.ArrayList;
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
 * Lucene-as-driver {@link FragmentConvertor}. Walks the resolved fragment, finds the
 * {@link OpenSearchFilter}, and serializes its condition as a {@link BoolQueryBuilder}'s
 * NamedWriteable bytes. Empty bytes when the fragment has no filter ({@code count(*)} over
 * MatchAllDocs at the data node).
 *
 * <p>Reuses the same leaf-serializer registry as {@link LuceneSubtreeConvertor} via
 * {@link QuerySerializerRegistry} — keyword equality, MATCH, MATCH_PHRASE, etc. all
 * round-trip through the same {@link DelegatedPredicateSerializer} → {@link QueryBuilder}
 * path. The data-node Lucene driver deserializes the bytes via NamedWriteable and runs
 * {@code IndexSearcher.count} on the resulting {@link QueryBuilder#toQuery(QueryShardContext)}.
 *
 * <p>Multi-stage / non-shard-scan fragments aren't supported: Lucene drives shard-local
 * count fragments only. Reduce or coordinator stages still run on DataFusion, so this
 * convertor is invoked only when the planner picked Lucene as the StagePlan's backend —
 * which happens exclusively for count-fast-path-eligible shards today.
 *
 * @opensearch.internal
 */
final class LuceneFragmentConvertor implements FragmentConvertor {

    private static final Logger LOGGER = LogManager.getLogger(LuceneFragmentConvertor.class);

    private final Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers;

    LuceneFragmentConvertor(Map<ScalarFunction, DelegatedPredicateSerializer> leafSerializers) {
        this.leafSerializers = leafSerializers;
    }

    /**
     * True iff the top is an {@link Aggregate} with empty group-set whose every call is
     * {@link SqlKind#COUNT} — what {@code IndexSearcher.count} can answer from the term
     * dictionary. Read by {@link LuceneShardPreference} to score this fragment.
     *
     * <p>Defense-in-depth: PlanForker's chain-agreement filter already narrows aggregate
     * alternatives to declared capabilities (prod Lucene declares only COUNT), so this
     * guards against capability-declaration drift.
     */
    static boolean isCountFastPath(RelNode fragment) {
        if (fragment instanceof Aggregate == false) return false;
        Aggregate agg = (Aggregate) fragment;
        if (agg.getGroupSet().isEmpty() == false) return false;
        for (AggregateCall call : agg.getAggCallList()) {
            if (call.getAggregation().getKind() != SqlKind.COUNT) return false;
        }
        return true;
    }

    @Override
    public byte[] convertFragment(RelNode fragment) {
        // Lucene-driver wire format: [columnNames StringCollection] [hasFilter boolean]
        // [QueryBuilder NamedWriteable]?. Both ends are controlled (this convertor on the
        // coordinator, LuceneScanInstructionHandler on the data node), so a tiny custom
        // format is fine — beats threading column names through the InstructionNode.
        // columnNames may be empty when the convertor runs against a non-count Lucene
        // alternative kept around for delegation (e.g. DF drives, Lucene is the peer); the
        // bytes are produced but the data node never invokes them — selector or runtime
        // alternative-selection drops this plan before dispatch.
        List<String> columnNames = extractAggCallNames(fragment);
        QueryBuilder filterQuery = null;
        Filter filter = findFilter(fragment);
        if (filter != null) {
            // strip() in FragmentConversionDriver replaces OpenSearchFilter with a plain
            // LogicalFilter, so the field-storage info lives on the OpenSearch ancestor
            // below (the TableScan). Walk down past LogicalFilter to find the nearest
            // OpenSearchRelNode and use its output field storage. The condition itself was
            // already resolved (annotation placeholders unwrapped) by the resolver in strip().
            List<FieldStorageInfo> fieldStorage = findFieldStorage(filter);
            filterQuery = toQueryBuilder(filter.getCondition(), fieldStorage);
        }
        byte[] bytes;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(columnNames);
            if (filterQuery == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeNamedWriteable(filterQuery);
            }
            bytes = BytesReference.toBytes(out.bytes());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene-driver fragment", e);
        }
        LOGGER.debug("[lucene-count] convertFragment columnNames={} filterQuery={} bytes={}", columnNames, filterQuery, bytes.length);
        return bytes;
    }

    /**
     * Walks down to find an Aggregate (Calcite {@link Aggregate} or {@code OpenSearchAggregate})
     * and extracts the user-facing call names. These become the Arrow output column names so
     * the coordinator's reduce sink sees the schema it expects.
     */
    private static List<String> extractAggCallNames(RelNode root) {
        RelNode current = root;
        while (current != null) {
            if (current instanceof Aggregate agg) {
                List<String> names = new ArrayList<>(agg.getAggCallList().size());
                for (AggregateCall call : agg.getAggCallList()) {
                    names.add(call.getName());
                }
                return names;
            }
            if (current.getInputs().isEmpty()) break;
            current = current.getInputs().getFirst();
        }
        return List.of();
    }

    @Override
    public byte[] attachPartialAggOnTop(RelNode partialAggFragment, byte[] innerBytes) {
        // Lucene-as-driver count fragments DO go through the partial-agg split — the driver's
        // FragmentConversionDriver invokes convertFragment on the input subtree (the
        // TableScan / Filter, no Aggregate above), then attachPartialAggOnTop on the
        // OpenSearchAggregate node. Without this rewrite, innerBytes carries an empty
        // columnNames list (extractAggCallNames found no Aggregate in the input) and the
        // data-node Lucene exec engine emits a 0-column Arrow batch — the coordinator
        // reduce sink then stalls waiting for the count column.
        //
        // Strategy: re-decode innerBytes' columnNames length-prefix (always present, possibly
        // empty), then preserve the remaining tail (hasFilter + optional QueryBuilder)
        // verbatim. Re-emit with the partialAggFragment's aggregate-call names as the new
        // columnNames. Avoids needing a NamedWriteableRegistry at coordinator-side conversion.
        if (!(partialAggFragment instanceof Aggregate agg)) {
            throw new IllegalStateException(
                "Lucene attachPartialAggOnTop expected an Aggregate fragment, got " + partialAggFragment.getClass().getSimpleName()
            );
        }
        List<String> columnNames = new ArrayList<>(agg.getAggCallList().size());
        for (AggregateCall call : agg.getAggCallList()) {
            columnNames.add(call.getName());
        }

        // Read past the inner columnNames StringCollection to get the byte offset of the
        // hasFilter + optional QueryBuilder tail. We then copy the tail verbatim into the new
        // bytes prefixed by the aggregate's column names.
        int tailOffset;
        try (StreamInput in = StreamInput.wrap(innerBytes)) {
            in.readStringList(); // discard inner columnNames; we'll write the agg names instead
            tailOffset = innerBytes.length - in.available();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to decode Lucene innerBytes during partial-agg attach", e);
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeStringCollection(columnNames);
            out.writeBytes(innerBytes, tailOffset, innerBytes.length - tailOffset);
            byte[] bytes = BytesReference.toBytes(out.bytes());
            LOGGER.debug("[lucene-count] attachPartialAggOnTop columnNames={} bytes={}", columnNames, bytes.length);
            return bytes;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to serialize Lucene-driver partial-agg bytes", e);
        }
    }

    @Override
    public WireFormat wireFormat() {
        // convertFragment emits a custom NamedWriteable wire format ([columnNames][hasFilter]
        // [BoolQueryBuilder]?), not self-describing. The orchestrator queries this so it
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
            default -> throw new IllegalStateException(
                "Lucene convertSchemaOnlyRead: unmapped Calcite type " + type.getSqlTypeName() + " for field of type " + type
            );
        };
    }

    /**
     * Walks the linear input chain looking for any Calcite {@link Filter} (covers both
     * {@link OpenSearchFilter} and the plain {@code LogicalFilter} that
     * {@code FragmentConversionDriver.strip} produces once annotation resolution unwraps the
     * filter's condition into native predicate calls).
     */
    private static Filter findFilter(RelNode node) {
        RelNode current = node;
        while (current != null) {
            if (current instanceof Filter filter) return filter;
            if (current.getInputs().isEmpty()) return null;
            current = current.getInputs().getFirst();
        }
        return null;
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
