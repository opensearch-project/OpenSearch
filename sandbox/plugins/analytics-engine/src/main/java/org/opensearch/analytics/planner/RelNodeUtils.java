/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchConvention;
import org.opensearch.analytics.planner.rel.OpenSearchDistribution;
import org.opensearch.analytics.planner.rel.OpenSearchDistributionTraitDef;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchJoin;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.planner.rel.OpenSearchUnion;
import org.opensearch.analytics.planner.rel.OpenSearchValues;
import org.opensearch.analytics.spi.FieldStorageInfo;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Copies an OpenSearch RelNode tree to a new cluster so all nodes register
 * with the new cluster's planner (Volcano).
 *
 * <p>Rebuilds distribution traits using the per-query {@link OpenSearchDistributionTraitDef}
 * so Calcite's identity-based trait matching works.
 *
 * <p>TODO: eliminate this by having frontends create RelNodes with the Volcano
 * cluster from the start.
 *
 * @opensearch.internal
 */
public class RelNodeUtils {

    private RelNodeUtils() {}

    /** Unwraps HepRelVertex to get the actual RelNode inside. */
    public static RelNode unwrapHep(RelNode node) {
        if (node instanceof HepRelVertex vertex) {
            return vertex.getCurrentRel();
        }
        return node;
    }

    public static RelNode copyToCluster(RelNode node, RelOptCluster newCluster, OpenSearchDistributionTraitDef distTraitDef) {
        List<RelNode> newInputs = node.getInputs().stream().map(input -> copyToCluster(input, newCluster, distTraitDef)).toList();

        RelTraitSet newTraits = rebuildTraits(node, newCluster, distTraitDef);

        if (node instanceof OpenSearchTableScan scan) {
            return new OpenSearchTableScan(newCluster, newTraits, scan.getTable(), scan.getViableBackends(), scan.getOutputFieldStorage());
        } else if (node instanceof OpenSearchFilter filter) {
            return new OpenSearchFilter(newCluster, newTraits, newInputs.getFirst(), filter.getCondition(), filter.getViableBackends());
        } else if (node instanceof OpenSearchAggregate aggregate) {
            return new OpenSearchAggregate(
                newCluster,
                newTraits,
                newInputs.getFirst(),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList(),
                aggregate.getMode(),
                aggregate.getViableBackends(),
                aggregate.getCallAnnotations(),
                aggregate.getFinalExtraLiteralArgs(),
                aggregate.getIntermediateFields()
            );
        } else if (node instanceof OpenSearchSort sort) {
            return new OpenSearchSort(
                newCluster,
                newTraits,
                newInputs.getFirst(),
                sort.getCollation(),
                sort.offset,
                sort.fetch,
                sort.getViableBackends()
            );
        } else if (node instanceof OpenSearchProject project) {
            return new OpenSearchProject(
                newCluster,
                newTraits,
                newInputs.getFirst(),
                project.getProjects(),
                project.getRowType(),
                project.getViableBackends()
            );
        } else if (node instanceof OpenSearchJoin join) {
            return new OpenSearchJoin(
                newCluster,
                newTraits,
                newInputs.get(0),
                newInputs.get(1),
                join.getCondition(),
                join.getJoinType(),
                join.getViableBackends()
            );
        } else if (node instanceof OpenSearchUnion union) {
            return new OpenSearchUnion(newCluster, newTraits, newInputs, union.all, union.getViableBackends());
        } else if (node instanceof OpenSearchValues values) {
            return new OpenSearchValues(newCluster, newTraits, values.getRowType(), values.getTuples(), values.getViableBackends());
        } else if (node instanceof OpenSearchExchangeReducer reducer) {
            return new OpenSearchExchangeReducer(
                newCluster,
                newTraits,
                newInputs.getFirst(),
                reducer.getViableBackends(),
                reducer.getExchangeInfo()
            );
        }

        throw new UnsupportedOperationException("Cannot copy node type: " + node.getClass().getSimpleName());
    }

    private static RelTraitSet rebuildTraits(RelNode node, RelOptCluster newCluster, OpenSearchDistributionTraitDef distTraitDef) {
        RelTraitSet traits = newCluster.traitSet().replace(OpenSearchConvention.INSTANCE);

        for (int index = 0; index < node.getTraitSet().size(); index++) {
            org.apache.calcite.plan.RelTrait trait = node.getTraitSet().getTrait(index);
            if (trait instanceof OpenSearchDistribution oldDist) {
                // Preserve the full distribution (kind, type, keys, tableId).
                traits = traits.replace(distTraitDef.from(oldDist));
            }
        }

        return traits;
    }

    /**
     * Finds the first node of the given type in the fragment's single-input chain.
     * Returns {@code null} if not found.
     *
     * <p>TODO: migrate existing findLeaf/findFilter usages in FragmentConversionDriver to use this.
     */
    @SuppressWarnings("unchecked")
    public static <T extends RelNode> T findNode(RelNode node, Class<T> type) {
        if (type.isInstance(node)) {
            return (T) node;
        }
        if (!node.getInputs().isEmpty()) {
            return findNode(node.getInputs().getFirst(), type);
        }
        return null;
    }

    /**
     * Qualified name of the first {@link OpenSearchTableScan} reachable from {@code node},
     * searching all inputs. Returns {@code null} if none is present.
     */
    public static String findTableName(RelNode node) {
        if (node == null) return null;
        if (node instanceof TableScan scan) {
            return scan.getTable().getQualifiedName().getLast();
        }
        for (RelNode input : node.getInputs()) {
            String name = findTableName(input);
            if (name != null) return name;
        }
        return null;
    }

    /** Maximum recursion depth when walking a RelNode tree to extract indices. */
    static final int MAX_EXTRACT_INDICES_DEPTH = 15;

    /**
     * Extracts all index names referenced by {@link org.apache.calcite.rel.core.TableScan}
     * nodes in the plan. Walks the tree up to {@link #MAX_EXTRACT_INDICES_DEPTH} levels to
     * guard against pathologically deep plans constructed from complex user queries.
     *
     * @param plan the root of the RelNode tree
     * @return array of distinct index names in encounter order
     * @throws IllegalStateException if the plan exceeds the maximum depth
     */
    public static String[] extractIndices(RelNode plan) {
        java.util.Set<String> indices = new java.util.LinkedHashSet<>();
        if (!collectIndices(plan, indices, 0)) {
            throw new IllegalStateException("Query plan exceeds maximum depth (" + MAX_EXTRACT_INDICES_DEPTH + ") for index extraction");
        }
        return indices.toArray(String[]::new);
    }

    private static boolean collectIndices(RelNode node, java.util.Set<String> indices, int depth) {
        if (depth >= MAX_EXTRACT_INDICES_DEPTH) {
            return false;
        }
        if (node instanceof TableScan scan) {
            java.util.List<String> names = scan.getTable().getQualifiedName();
            indices.add(names.get(names.size() - 1));
        }
        for (RelNode input : node.getInputs()) {
            if (!collectIndices(input, indices, depth + 1)) {
                return false;
            }
        }
        return true;
    }

    /** Collects every {@link RexInputRef} index appearing inside a {@link RexNode} tree. */
    public static Set<Integer> collectInputRefs(RexNode node) {
        Set<Integer> out = new HashSet<>();
        node.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                out.add(ref.getIndex());
                return ref;
            }
        });
        return out;
    }

    /**
     * Resolves a derived expression to the ordered list of physical-field names it depends on,
     * deduped by first-appearance. Used by {@link OpenSearchProject#getOutputFieldStorage} and
     * {@link OpenSearchAggregate#getOutputFieldStorage} to populate
     * {@link FieldStorageInfo#getDependsOnPhysicalCols} per Invariant 1 of the QTF v2 algorithm.
     *
     * <p>For each {@code RexInputRef} encountered (depth-first order):
     * <ul>
     *   <li>If the input FSI at that index is non-derived, add its field name.</li>
     *   <li>If the input FSI at that index is derived, recurse into its
     *       {@code dependsOnPhysicalCols} (already resolved by the upstream operator).</li>
     * </ul>
     */
    public static LinkedHashSet<String> resolvePhysicalDeps(RexNode node, List<FieldStorageInfo> inputStorage) {
        LinkedHashSet<String> deps = new LinkedHashSet<>();
        node.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                int idx = ref.getIndex();
                if (idx >= inputStorage.size()) {
                    throw new IllegalStateException(
                        "RexInputRef["
                            + idx
                            + "] has no matching FieldStorageInfo entry "
                            + "(input only declares "
                            + inputStorage.size()
                            + " columns) — "
                            + "the upstream operator did not record storage for every output column"
                    );
                }
                FieldStorageInfo src = inputStorage.get(idx);
                if (src.isDerived()) {
                    deps.addAll(src.getDependsOnPhysicalCols());
                } else {
                    deps.add(src.getFieldName());
                }
                return ref;
            }
        });
        return deps;
    }

    /**
     * Returns a copy of {@code base} with one extra field {@code (name, type)} appended.
     * Used by rewrites that augment a rowType with synthetic helper columns.
     */
    public static RelDataType appendField(RelDataTypeFactory typeFactory, RelDataType base, String name, RelDataType type) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        for (RelDataTypeField f : base.getFieldList()) {
            builder.add(f.getName(), f.getType());
        }
        builder.add(name, type);
        return builder.build();
    }

    /**
     * {@link RexShuttle} that rewrites every {@link RexInputRef} via {@code remap[oldIdx]}.
     * Throws when {@code remap[oldIdx] < 0} (referenced column was dropped). Output ref's
     * type is sourced from {@code newRowType}.
     */
    public static final class IndexRemapShuttle extends RexShuttle {
        private final int[] remap;
        private final RelDataType newRowType;

        public IndexRemapShuttle(int[] remap, RelDataType newRowType) {
            this.remap = remap;
            this.newRowType = newRowType;
        }

        @Override
        public RexNode visitInputRef(RexInputRef ref) {
            int newIdx = remap[ref.getIndex()];
            if (newIdx < 0) {
                throw new IllegalStateException("RexInputRef references dropped column at original idx " + ref.getIndex());
            }
            return new RexInputRef(newIdx, newRowType.getFieldList().get(newIdx).getType());
        }
    }
}
