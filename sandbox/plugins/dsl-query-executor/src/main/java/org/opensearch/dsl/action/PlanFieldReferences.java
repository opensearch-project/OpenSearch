/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.opensearch.dsl.executor.QueryPlans;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Extracts the schema-equivalence gate's inputs from a request: the base index-mapping fields the
 * converted plans actually reference, and the subset that are terms-aggregation bucket keys.
 *
 * <p>Referenced fields are derived from the {@link RelNode} plans rather than the whole schema, so
 * a field that diverges across indices but is never referenced does not trigger a rejection. Only
 * leaf {@link TableScan} columns (the index-mapping-derived schema fields) count as base fields;
 * intermediate computed columns are excluded.
 */
final class PlanFieldReferences {

    private PlanFieldReferences() {}

    /** Collects the base index-mapping field names referenced anywhere in the plans. */
    static Set<String> referencedFields(QueryPlans plans) {
        Set<String> baseNames = new HashSet<>();
        RelVisitor scanCollector = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    baseNames.addAll(node.getRowType().getFieldNames());
                }
                super.visit(node, ordinal, parent);
            }
        };
        for (QueryPlans.QueryPlan plan : plans.getAll()) {
            scanCollector.go(plan.relNode());
        }

        Set<String> referenced = new HashSet<>();
        RelVisitor refCollector = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                collectNodeReferences(node, baseNames, referenced);
                super.visit(node, ordinal, parent);
            }
        };
        for (QueryPlans.QueryPlan plan : plans.getAll()) {
            refCollector.go(plan.relNode());
        }
        return referenced;
    }

    /** Collects the (dotted) field names of every terms aggregation in the request, at any nesting depth. */
    static Set<String> aggregatedBucketFields(SearchSourceBuilder searchSource) {
        Set<String> fields = new HashSet<>();
        if (searchSource.aggregations() != null) {
            collectTermsFields(searchSource.aggregations().getAggregatorFactories(), fields);
        }
        return fields;
    }

    private static void collectTermsFields(Collection<AggregationBuilder> aggregations, Set<String> fields) {
        if (aggregations == null) {
            return;
        }
        for (AggregationBuilder aggregation : aggregations) {
            if (aggregation instanceof TermsAggregationBuilder terms && terms.field() != null) {
                fields.add(terms.field());
            }
            collectTermsFields(aggregation.getSubAggregations(), fields);
        }
    }

    /**
     * Adds the base-column names one node references. {@link RexInputRef}s in the node's own
     * expressions are captured through a {@link RexShuttle}; {@link Aggregate} grouping/call
     * ordinals and {@link Sort} collation ordinals carry no {@code RexInputRef} and are read
     * directly. Every ordinal is resolved to a name against the concatenated row type of the
     * node's inputs — the exact space {@code RexInputRef} indices address.
     */
    private static void collectNodeReferences(RelNode node, Set<String> baseNames, Set<String> referenced) {
        List<String> inputFieldNames = new ArrayList<>();
        for (RelNode input : node.getInputs()) {
            inputFieldNames.addAll(input.getRowType().getFieldNames());
        }

        node.accept(new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                addReference(inputFieldNames, inputRef.getIndex(), baseNames, referenced);
                return inputRef;
            }
        });

        if (node instanceof Aggregate aggregate) {
            for (int groupField : aggregate.getGroupSet()) {
                addReference(inputFieldNames, groupField, baseNames, referenced);
            }
            for (AggregateCall call : aggregate.getAggCallList()) {
                for (int argument : call.getArgList()) {
                    addReference(inputFieldNames, argument, baseNames, referenced);
                }
                if (call.filterArg >= 0) {
                    addReference(inputFieldNames, call.filterArg, baseNames, referenced);
                }
            }
        } else if (node instanceof Sort sort) {
            for (RelFieldCollation collation : sort.getCollation().getFieldCollations()) {
                addReference(inputFieldNames, collation.getFieldIndex(), baseNames, referenced);
            }
        }
    }

    private static void addReference(List<String> inputFieldNames, int index, Set<String> baseNames, Set<String> referenced) {
        if (index >= 0 && index < inputFieldNames.size()) {
            String name = inputFieldNames.get(index);
            if (baseNames.contains(name)) {
                referenced.add(name);
            }
        }
    }
}
