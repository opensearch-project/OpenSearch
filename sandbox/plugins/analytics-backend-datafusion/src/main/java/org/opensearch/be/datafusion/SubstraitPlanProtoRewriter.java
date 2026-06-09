/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import io.substrait.proto.CrossRel;
import io.substrait.proto.ExtensionMultiRel;
import io.substrait.proto.HashJoinRel;
import io.substrait.proto.JoinRel;
import io.substrait.proto.MergeJoinRel;
import io.substrait.proto.NamedStruct;
import io.substrait.proto.NestedLoopJoinRel;
import io.substrait.proto.Plan;
import io.substrait.proto.PlanRel;
import io.substrait.proto.ReadRel;
import io.substrait.proto.Rel;
import io.substrait.proto.RelRoot;
import io.substrait.proto.SetRel;
import io.substrait.proto.Type;

/**
 * Proto-layer Substrait plan rewriter. Runs after {@code SubstraitPlanPojoRewriter} serializes
 * the POJO so it can patch fields the POJO API doesn't expose. Today it covers two cases, both
 * confined to {@code ReadRel.base_schema} type nodes:
 *
 * <ol>
 *   <li><b>PrecisionTime variation.</b> {@code Type.PrecisionTime} arrives with
 *       {@code type_variation_reference = 0} (TIME_32) regardless of the precision, because
 *       substrait-java's POJO has no setter for that field. For {@code precision ∈ {6, 9}} that
 *       yields {@code Time32(µs|ns)} — an Arrow type that doesn't exist; the DataFusion consumer
 *       builds it anyway and rejects the schema match against the real {@code Time64} column.
 *       Promoting to variation 1 (TIME_64) is unconditionally correct: the Time32 form for those
 *       precisions has no valid representation, so there's nothing to lose.</li>
 *   <li><b>base_schema nullability.</b> Every {@code base_schema} field type is forced to
 *       {@code NULLABILITY_NULLABLE}. The nullability our Java planner emits is Calcite-derived:
 *       aggregate outputs ({@code count()}), and internal/union-fill columns ({@code __seg_id__},
 *       alias fan-out columns) are marked {@code NOT NULL}. The Rust {@code TableProvider}s backing
 *       those reads legitimately expose the same columns as nullable (the parquet {@code SchemaAdapter}
 *       null-fills columns a shard lacks; aggregate columns surface nullable). DataFusion 53's
 *       substrait consumer forced every {@code base_schema} field nullable on the way in, so the
 *       mismatch never surfaced. DataFusion 54 now honors the declared flag and rejects the read
 *       with <em>"Field 'x' is nullable in the DataFusion schema but not nullable in the Substrait
 *       schema."</em> Forcing {@code base_schema} fields nullable relaxes only the nullability
 *       assertion — the consumer still validates field types and the column set — restoring the
 *       behavior we shipped against. Actual data nullability is governed by the provider schema,
 *       not by {@code base_schema}.</li>
 * </ol>
 *
 * <p>Walks {@code Plan.relations} → every Rel variant's inputs →
 * {@code ReadRel.base_schema} → nested Struct / List / Map element types.
 *
 * <p>TODO: remove the PrecisionTime promotion once substrait-java exposes
 * {@code typeVariationReference} on {@code Type.PrecisionTime}.
 *
 * @opensearch.internal
 */
final class SubstraitPlanProtoRewriter {

    private static final int TIME_64_TYPE_VARIATION_REF = 1;

    private SubstraitPlanProtoRewriter() {}

    /**
     * Walks {@code plan} and returns a copy with every offending
     * {@code PrecisionTime} promoted to TIME_64. Returns the original instance
     * unchanged if no rewrite was needed (so the common no-op case avoids an
     * extra builder allocation).
     */
    static Plan rewrite(Plan plan) {
        Plan.Builder builder = null;
        for (int i = 0; i < plan.getRelationsCount(); i++) {
            PlanRel original = plan.getRelations(i);
            PlanRel rewritten = rewritePlanRel(original);
            if (rewritten != original) {
                if (builder == null) {
                    builder = plan.toBuilder();
                }
                builder.setRelations(i, rewritten);
            }
        }
        return builder != null ? builder.build() : plan;
    }

    private static PlanRel rewritePlanRel(PlanRel pr) {
        switch (pr.getRelTypeCase()) {
            case REL: {
                Rel rewritten = rewriteRel(pr.getRel());
                return rewritten != pr.getRel() ? pr.toBuilder().setRel(rewritten).build() : pr;
            }
            case ROOT: {
                RelRoot root = pr.getRoot();
                if (!root.hasInput()) {
                    return pr;
                }
                Rel rewritten = rewriteRel(root.getInput());
                if (rewritten == root.getInput()) {
                    return pr;
                }
                return pr.toBuilder().setRoot(root.toBuilder().setInput(rewritten).build()).build();
            }
            case RELTYPE_NOT_SET:
            default:
                return pr;
        }
    }

    private static Rel rewriteRel(Rel rel) {
        switch (rel.getRelTypeCase()) {
            case READ: {
                ReadRel rewritten = rewriteRead(rel.getRead());
                return rewritten != rel.getRead() ? rel.toBuilder().setRead(rewritten).build() : rel;
            }
            case FILTER: {
                if (!rel.getFilter().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getFilter().getInput());
                if (inp == rel.getFilter().getInput()) return rel;
                return rel.toBuilder().setFilter(rel.getFilter().toBuilder().setInput(inp).build()).build();
            }
            case FETCH: {
                if (!rel.getFetch().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getFetch().getInput());
                if (inp == rel.getFetch().getInput()) return rel;
                return rel.toBuilder().setFetch(rel.getFetch().toBuilder().setInput(inp).build()).build();
            }
            case AGGREGATE: {
                if (!rel.getAggregate().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getAggregate().getInput());
                if (inp == rel.getAggregate().getInput()) return rel;
                return rel.toBuilder().setAggregate(rel.getAggregate().toBuilder().setInput(inp).build()).build();
            }
            case SORT: {
                if (!rel.getSort().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getSort().getInput());
                if (inp == rel.getSort().getInput()) return rel;
                return rel.toBuilder().setSort(rel.getSort().toBuilder().setInput(inp).build()).build();
            }
            case JOIN: {
                Rel l = rel.getJoin().hasLeft() ? rewriteRel(rel.getJoin().getLeft()) : null;
                Rel r = rel.getJoin().hasRight() ? rewriteRel(rel.getJoin().getRight()) : null;
                boolean lc = l != null && l != rel.getJoin().getLeft();
                boolean rc = r != null && r != rel.getJoin().getRight();
                if (!lc && !rc) return rel;
                JoinRel.Builder jb = rel.getJoin().toBuilder();
                if (lc) jb.setLeft(l);
                if (rc) jb.setRight(r);
                return rel.toBuilder().setJoin(jb.build()).build();
            }
            case PROJECT: {
                if (!rel.getProject().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getProject().getInput());
                if (inp == rel.getProject().getInput()) return rel;
                return rel.toBuilder().setProject(rel.getProject().toBuilder().setInput(inp).build()).build();
            }
            case SET: {
                SetRel set = rel.getSet();
                SetRel.Builder sb = null;
                for (int i = 0; i < set.getInputsCount(); i++) {
                    Rel inp = rewriteRel(set.getInputs(i));
                    if (inp != set.getInputs(i)) {
                        if (sb == null) sb = set.toBuilder();
                        sb.setInputs(i, inp);
                    }
                }
                return sb != null ? rel.toBuilder().setSet(sb.build()).build() : rel;
            }
            case EXTENSION_SINGLE: {
                if (!rel.getExtensionSingle().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getExtensionSingle().getInput());
                if (inp == rel.getExtensionSingle().getInput()) return rel;
                return rel.toBuilder().setExtensionSingle(rel.getExtensionSingle().toBuilder().setInput(inp).build()).build();
            }
            case EXTENSION_MULTI: {
                ExtensionMultiRel em = rel.getExtensionMulti();
                ExtensionMultiRel.Builder eb = null;
                for (int i = 0; i < em.getInputsCount(); i++) {
                    Rel inp = rewriteRel(em.getInputs(i));
                    if (inp != em.getInputs(i)) {
                        if (eb == null) eb = em.toBuilder();
                        eb.setInputs(i, inp);
                    }
                }
                return eb != null ? rel.toBuilder().setExtensionMulti(eb.build()).build() : rel;
            }
            case CROSS: {
                Rel l = rel.getCross().hasLeft() ? rewriteRel(rel.getCross().getLeft()) : null;
                Rel r = rel.getCross().hasRight() ? rewriteRel(rel.getCross().getRight()) : null;
                boolean lc = l != null && l != rel.getCross().getLeft();
                boolean rc = r != null && r != rel.getCross().getRight();
                if (!lc && !rc) return rel;
                CrossRel.Builder cb = rel.getCross().toBuilder();
                if (lc) cb.setLeft(l);
                if (rc) cb.setRight(r);
                return rel.toBuilder().setCross(cb.build()).build();
            }
            case HASH_JOIN: {
                Rel l = rel.getHashJoin().hasLeft() ? rewriteRel(rel.getHashJoin().getLeft()) : null;
                Rel r = rel.getHashJoin().hasRight() ? rewriteRel(rel.getHashJoin().getRight()) : null;
                boolean lc = l != null && l != rel.getHashJoin().getLeft();
                boolean rc = r != null && r != rel.getHashJoin().getRight();
                if (!lc && !rc) return rel;
                HashJoinRel.Builder hb = rel.getHashJoin().toBuilder();
                if (lc) hb.setLeft(l);
                if (rc) hb.setRight(r);
                return rel.toBuilder().setHashJoin(hb.build()).build();
            }
            case MERGE_JOIN: {
                Rel l = rel.getMergeJoin().hasLeft() ? rewriteRel(rel.getMergeJoin().getLeft()) : null;
                Rel r = rel.getMergeJoin().hasRight() ? rewriteRel(rel.getMergeJoin().getRight()) : null;
                boolean lc = l != null && l != rel.getMergeJoin().getLeft();
                boolean rc = r != null && r != rel.getMergeJoin().getRight();
                if (!lc && !rc) return rel;
                MergeJoinRel.Builder mb = rel.getMergeJoin().toBuilder();
                if (lc) mb.setLeft(l);
                if (rc) mb.setRight(r);
                return rel.toBuilder().setMergeJoin(mb.build()).build();
            }
            case NESTED_LOOP_JOIN: {
                Rel l = rel.getNestedLoopJoin().hasLeft() ? rewriteRel(rel.getNestedLoopJoin().getLeft()) : null;
                Rel r = rel.getNestedLoopJoin().hasRight() ? rewriteRel(rel.getNestedLoopJoin().getRight()) : null;
                boolean lc = l != null && l != rel.getNestedLoopJoin().getLeft();
                boolean rc = r != null && r != rel.getNestedLoopJoin().getRight();
                if (!lc && !rc) return rel;
                NestedLoopJoinRel.Builder nb = rel.getNestedLoopJoin().toBuilder();
                if (lc) nb.setLeft(l);
                if (rc) nb.setRight(r);
                return rel.toBuilder().setNestedLoopJoin(nb.build()).build();
            }
            case WINDOW: {
                if (!rel.getWindow().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getWindow().getInput());
                if (inp == rel.getWindow().getInput()) return rel;
                return rel.toBuilder().setWindow(rel.getWindow().toBuilder().setInput(inp).build()).build();
            }
            case EXCHANGE: {
                if (!rel.getExchange().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getExchange().getInput());
                if (inp == rel.getExchange().getInput()) return rel;
                return rel.toBuilder().setExchange(rel.getExchange().toBuilder().setInput(inp).build()).build();
            }
            case EXPAND: {
                if (!rel.getExpand().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getExpand().getInput());
                if (inp == rel.getExpand().getInput()) return rel;
                return rel.toBuilder().setExpand(rel.getExpand().toBuilder().setInput(inp).build()).build();
            }
            case WRITE: {
                if (!rel.getWrite().hasInput()) return rel;
                Rel inp = rewriteRel(rel.getWrite().getInput());
                if (inp == rel.getWrite().getInput()) return rel;
                return rel.toBuilder().setWrite(rel.getWrite().toBuilder().setInput(inp).build()).build();
            }
            // Leaf rels — no inputs to recurse, no schema we rewrite here today.
            case EXTENSION_LEAF:
            case REFERENCE:
            case DDL:
            case UPDATE:
            case RELTYPE_NOT_SET:
            default:
                return rel;
        }
    }

    private static ReadRel rewriteRead(ReadRel read) {
        if (!read.hasBaseSchema()) {
            return read;
        }
        // Nullability forcing applies ONLY to NamedTable reads — those are the ones whose base_schema
        // the DataFusion consumer matches (by name) against a registered TableProvider, where the
        // Calcite-derived NOT NULL flags clash with the providers' nullable columns. A VirtualTable
        // read carries inline row literals that substrait-java's VirtualTableScan.check() asserts must
        // match the base_schema field types exactly (including nullability); relaxing the schema there
        // would break that self-consistency check. The PrecisionTime promotion, by contrast, is always
        // safe and applies to every read type.
        boolean forceNull = read.getReadTypeCase() == ReadRel.ReadTypeCase.NAMED_TABLE;
        NamedStruct rewritten = rewriteNamedStruct(read.getBaseSchema(), forceNull);
        return rewritten != read.getBaseSchema() ? read.toBuilder().setBaseSchema(rewritten).build() : read;
    }

    private static NamedStruct rewriteNamedStruct(NamedStruct ns, boolean forceNull) {
        if (!ns.hasStruct()) {
            return ns;
        }
        Type.Struct rewritten = rewriteStruct(ns.getStruct(), forceNull);
        return rewritten != ns.getStruct() ? ns.toBuilder().setStruct(rewritten).build() : ns;
    }

    private static Type.Struct rewriteStruct(Type.Struct s, boolean forceNull) {
        Type.Struct.Builder sb = null;
        for (int i = 0; i < s.getTypesCount(); i++) {
            Type rewritten = rewriteType(s.getTypes(i), forceNull);
            if (rewritten != s.getTypes(i)) {
                if (sb == null) sb = s.toBuilder();
                sb.setTypes(i, rewritten);
            }
        }
        return sb != null ? sb.build() : s;
    }

    /**
     * Rewrites a {@code base_schema} field type for a NamedTable read: applies the structural fixes
     * ({@link #promoteStructure}) and then forces the node's nullability to
     * {@code NULLABILITY_NULLABLE} ({@link #forceNullable}). Both passes preserve the
     * same-instance no-op optimization, so a type that needs neither fix round-trips unchanged.
     *
     * <p>Visible for tests. Tests exercise the NamedTable behavior (nullability forced on), which is
     * the case the rewriter exists to fix.
     */
    static Type rewriteType(Type t) {
        return rewriteType(t, true);
    }

    private static Type rewriteType(Type t, boolean forceNull) {
        Type structural = promoteStructure(t, forceNull);
        return forceNull ? forceNullable(structural) : structural;
    }

    /**
     * Structural rewrites that don't touch nullability: PrecisionTime variation promotion plus
     * recursion into Struct / List / Map element types. Children are routed back through
     * {@link #rewriteType} so they receive the same nullability treatment ({@code forceNull}) as
     * the parent.
     */
    private static Type promoteStructure(Type t, boolean forceNull) {
        switch (t.getKindCase()) {
            case PRECISION_TIME: {
                Type.PrecisionTime pt = t.getPrecisionTime();
                if (pt.getTypeVariationReference() == 0 && (pt.getPrecision() == 6 || pt.getPrecision() == 9)) {
                    return t.toBuilder()
                        .setPrecisionTime(pt.toBuilder().setTypeVariationReference(TIME_64_TYPE_VARIATION_REF).build())
                        .build();
                }
                return t;
            }
            case STRUCT: {
                Type.Struct rewritten = rewriteStruct(t.getStruct(), forceNull);
                return rewritten != t.getStruct() ? t.toBuilder().setStruct(rewritten).build() : t;
            }
            case LIST: {
                if (!t.getList().hasType()) return t;
                Type inner = rewriteType(t.getList().getType(), forceNull);
                if (inner == t.getList().getType()) return t;
                return t.toBuilder().setList(t.getList().toBuilder().setType(inner).build()).build();
            }
            case MAP: {
                Type.Map.Builder mb = null;
                Type.Map m = t.getMap();
                if (m.hasKey()) {
                    Type k = rewriteType(m.getKey(), forceNull);
                    if (k != m.getKey()) {
                        mb = m.toBuilder();
                        mb.setKey(k);
                    }
                }
                if (m.hasValue()) {
                    Type v = rewriteType(m.getValue(), forceNull);
                    if (v != m.getValue()) {
                        if (mb == null) mb = m.toBuilder();
                        mb.setValue(v);
                    }
                }
                return mb != null ? t.toBuilder().setMap(mb.build()).build() : t;
            }
            default:
                return t;
        }
    }

    /**
     * Forces {@code t}'s own nullability to {@code NULLABILITY_NULLABLE}, returning the same
     * instance if it is already nullable (so a fully-nullable schema round-trips unchanged).
     *
     * <p>Non-recursive: nested Struct / List / Map element types are forced nullable by
     * {@link #promoteStructure} routing each child back through {@link #rewriteType}. Nullability
     * is stored per-kind in the Substrait proto with no shared accessor, so this is an exhaustive
     * switch rather than a single set call. The deprecated temporal kinds are handled too —
     * substrait-java emits the {@code Precision*} variants today, but covering them keeps the pass
     * complete if an upstream change ever resurrects them.
     *
     * <p>{@code USER_DEFINED_TYPE_REFERENCE} (a bare deprecated int, not a message) and
     * {@code KIND_NOT_SET} carry no nullability field and fall through unchanged.
     */
    @SuppressWarnings("deprecation") // touches deprecated Timestamp/Time/TimestampTZ kinds for completeness
    private static Type forceNullable(Type t) {
        final Type.Nullability nullable = Type.Nullability.NULLABILITY_NULLABLE;
        switch (t.getKindCase()) {
            case BOOL:
                if (t.getBool().getNullability() == nullable) return t;
                return t.toBuilder().setBool(t.getBool().toBuilder().setNullability(nullable).build()).build();
            case I8:
                if (t.getI8().getNullability() == nullable) return t;
                return t.toBuilder().setI8(t.getI8().toBuilder().setNullability(nullable).build()).build();
            case I16:
                if (t.getI16().getNullability() == nullable) return t;
                return t.toBuilder().setI16(t.getI16().toBuilder().setNullability(nullable).build()).build();
            case I32:
                if (t.getI32().getNullability() == nullable) return t;
                return t.toBuilder().setI32(t.getI32().toBuilder().setNullability(nullable).build()).build();
            case I64:
                if (t.getI64().getNullability() == nullable) return t;
                return t.toBuilder().setI64(t.getI64().toBuilder().setNullability(nullable).build()).build();
            case FP32:
                if (t.getFp32().getNullability() == nullable) return t;
                return t.toBuilder().setFp32(t.getFp32().toBuilder().setNullability(nullable).build()).build();
            case FP64:
                if (t.getFp64().getNullability() == nullable) return t;
                return t.toBuilder().setFp64(t.getFp64().toBuilder().setNullability(nullable).build()).build();
            case STRING:
                if (t.getString().getNullability() == nullable) return t;
                return t.toBuilder().setString(t.getString().toBuilder().setNullability(nullable).build()).build();
            case BINARY:
                if (t.getBinary().getNullability() == nullable) return t;
                return t.toBuilder().setBinary(t.getBinary().toBuilder().setNullability(nullable).build()).build();
            case TIMESTAMP:
                if (t.getTimestamp().getNullability() == nullable) return t;
                return t.toBuilder().setTimestamp(t.getTimestamp().toBuilder().setNullability(nullable).build()).build();
            case DATE:
                if (t.getDate().getNullability() == nullable) return t;
                return t.toBuilder().setDate(t.getDate().toBuilder().setNullability(nullable).build()).build();
            case TIME:
                if (t.getTime().getNullability() == nullable) return t;
                return t.toBuilder().setTime(t.getTime().toBuilder().setNullability(nullable).build()).build();
            case INTERVAL_YEAR:
                if (t.getIntervalYear().getNullability() == nullable) return t;
                return t.toBuilder().setIntervalYear(t.getIntervalYear().toBuilder().setNullability(nullable).build()).build();
            case INTERVAL_DAY:
                if (t.getIntervalDay().getNullability() == nullable) return t;
                return t.toBuilder().setIntervalDay(t.getIntervalDay().toBuilder().setNullability(nullable).build()).build();
            case INTERVAL_COMPOUND:
                if (t.getIntervalCompound().getNullability() == nullable) return t;
                return t.toBuilder()
                    .setIntervalCompound(t.getIntervalCompound().toBuilder().setNullability(nullable).build())
                    .build();
            case TIMESTAMP_TZ:
                if (t.getTimestampTz().getNullability() == nullable) return t;
                return t.toBuilder().setTimestampTz(t.getTimestampTz().toBuilder().setNullability(nullable).build()).build();
            case UUID:
                if (t.getUuid().getNullability() == nullable) return t;
                return t.toBuilder().setUuid(t.getUuid().toBuilder().setNullability(nullable).build()).build();
            case FIXED_CHAR:
                if (t.getFixedChar().getNullability() == nullable) return t;
                return t.toBuilder().setFixedChar(t.getFixedChar().toBuilder().setNullability(nullable).build()).build();
            case VARCHAR:
                if (t.getVarchar().getNullability() == nullable) return t;
                return t.toBuilder().setVarchar(t.getVarchar().toBuilder().setNullability(nullable).build()).build();
            case FIXED_BINARY:
                if (t.getFixedBinary().getNullability() == nullable) return t;
                return t.toBuilder().setFixedBinary(t.getFixedBinary().toBuilder().setNullability(nullable).build()).build();
            case DECIMAL:
                if (t.getDecimal().getNullability() == nullable) return t;
                return t.toBuilder().setDecimal(t.getDecimal().toBuilder().setNullability(nullable).build()).build();
            case PRECISION_TIME:
                if (t.getPrecisionTime().getNullability() == nullable) return t;
                return t.toBuilder().setPrecisionTime(t.getPrecisionTime().toBuilder().setNullability(nullable).build()).build();
            case PRECISION_TIMESTAMP:
                if (t.getPrecisionTimestamp().getNullability() == nullable) return t;
                return t.toBuilder()
                    .setPrecisionTimestamp(t.getPrecisionTimestamp().toBuilder().setNullability(nullable).build())
                    .build();
            case PRECISION_TIMESTAMP_TZ:
                if (t.getPrecisionTimestampTz().getNullability() == nullable) return t;
                return t.toBuilder()
                    .setPrecisionTimestampTz(t.getPrecisionTimestampTz().toBuilder().setNullability(nullable).build())
                    .build();
            case STRUCT:
                if (t.getStruct().getNullability() == nullable) return t;
                return t.toBuilder().setStruct(t.getStruct().toBuilder().setNullability(nullable).build()).build();
            case LIST:
                if (t.getList().getNullability() == nullable) return t;
                return t.toBuilder().setList(t.getList().toBuilder().setNullability(nullable).build()).build();
            case MAP:
                if (t.getMap().getNullability() == nullable) return t;
                return t.toBuilder().setMap(t.getMap().toBuilder().setNullability(nullable).build()).build();
            case FUNC:
                if (t.getFunc().getNullability() == nullable) return t;
                return t.toBuilder().setFunc(t.getFunc().toBuilder().setNullability(nullable).build()).build();
            case USER_DEFINED:
                if (t.getUserDefined().getNullability() == nullable) return t;
                return t.toBuilder().setUserDefined(t.getUserDefined().toBuilder().setNullability(nullable).build()).build();
            case ALIAS:
                if (t.getAlias().getNullability() == nullable) return t;
                return t.toBuilder().setAlias(t.getAlias().toBuilder().setNullability(nullable).build()).build();
            // No nullability field to set (deprecated bare-int reference, or empty oneof).
            case USER_DEFINED_TYPE_REFERENCE:
            case KIND_NOT_SET:
            default:
                return t;
        }
    }
}
