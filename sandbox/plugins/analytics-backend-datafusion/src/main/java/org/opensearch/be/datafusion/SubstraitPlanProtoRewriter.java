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
 * the POJO so it can patch fields the POJO API doesn't expose. Today it covers a single case:
 * {@code Type.PrecisionTime} arrives with {@code type_variation_reference = 0}
 * (TIME_32) regardless of the precision, because substrait-java's POJO has no
 * setter for that field. For {@code precision ∈ {6, 9}} that yields
 * {@code Time32(µs|ns)} — an Arrow type that doesn't exist; the DataFusion consumer
 * builds it anyway and rejects the schema match against the real {@code Time64}
 * column. Promoting to variation 1 (TIME_64) is unconditionally correct: the
 * Time32 form for those precisions has no valid representation, so there's nothing
 * to lose.
 *
 * <p>Walks {@code Plan.relations} → every Rel variant's inputs →
 * {@code ReadRel.base_schema} → nested Struct / List / Map element types.
 *
 * <p>TODO: remove this class once substrait-java exposes
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
        NamedStruct rewritten = rewriteNamedStruct(read.getBaseSchema());
        return rewritten != read.getBaseSchema() ? read.toBuilder().setBaseSchema(rewritten).build() : read;
    }

    private static NamedStruct rewriteNamedStruct(NamedStruct ns) {
        if (!ns.hasStruct()) {
            return ns;
        }
        Type.Struct rewritten = rewriteStruct(ns.getStruct());
        return rewritten != ns.getStruct() ? ns.toBuilder().setStruct(rewritten).build() : ns;
    }

    private static Type.Struct rewriteStruct(Type.Struct s) {
        Type.Struct.Builder sb = null;
        for (int i = 0; i < s.getTypesCount(); i++) {
            Type rewritten = rewriteType(s.getTypes(i));
            if (rewritten != s.getTypes(i)) {
                if (sb == null) sb = s.toBuilder();
                sb.setTypes(i, rewritten);
            }
        }
        return sb != null ? sb.build() : s;
    }

    /**
     * Visible for tests.
     */
    static Type rewriteType(Type t) {
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
                Type.Struct rewritten = rewriteStruct(t.getStruct());
                return rewritten != t.getStruct() ? t.toBuilder().setStruct(rewritten).build() : t;
            }
            case LIST: {
                if (!t.getList().hasType()) return t;
                Type inner = rewriteType(t.getList().getType());
                if (inner == t.getList().getType()) return t;
                return t.toBuilder().setList(t.getList().toBuilder().setType(inner).build()).build();
            }
            case MAP: {
                Type.Map.Builder mb = null;
                Type.Map m = t.getMap();
                if (m.hasKey()) {
                    Type k = rewriteType(m.getKey());
                    if (k != m.getKey()) {
                        mb = m.toBuilder();
                        mb.setKey(k);
                    }
                }
                if (m.hasValue()) {
                    Type v = rewriteType(m.getValue());
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
}
