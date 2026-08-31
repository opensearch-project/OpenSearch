/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.RelNode;
import org.opensearch.dsl.aggregation.AggregationMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * One or more query plans produced by DSL to RelNode conversion.
 */
public final class QueryPlans {

    /** Identifies what part of the SearchResponse a plan populates. */
    public enum Type {
        /** Document hits. */
        HITS,
        /** Aggregation results. */
        AGGREGATION,
        /**
         * Request totals: single-row aggregates supplying the counts that plan-level LIMITs
         * remove from the main plans — {@code hits.total} and the {@code sum_other_doc_count}
         * eligible-doc counts. A request may carry several COUNT plans (one flat plan for
         * {@code COUNT(*)}/{@code COUNT(field)} columns, plus one per {@code min_doc_count}
         * aggregation whose eligible count needs a HAVING-filtered sum). All execute concurrently
         * with the main plans.
         */
        COUNT
    }

    /** Column name of the COUNT plans' {@code COUNT(*)} — total docs matching the query. */
    public static final String COUNT_TOTAL_COLUMN = "_total";

    /**
     * Column name prefix of a root sized aggregation's eligible-doc count, the total {@code sum_other_doc_count} is subtracted from:
     * the documents eligible for its buckets, named {@code _eligible$<aggregationName>} (root
     * names are unique among siblings by DSL contract). For {@code min_doc_count} ≤ 1 this is
     * {@code COUNT(field)}; for higher thresholds it is the sum of counts over the
     * HAVING-filtered groups, delivered by that aggregation's own COUNT plan.
     */
    public static final String COUNT_ELIGIBLE_COLUMN_PREFIX = "_eligible$";

    /**
     * A single plan pairing a {@link Type} with a Calcite {@link RelNode}.
     *
     * @param type what part of the response this plan produces
     * @param relNode the Calcite logical plan to execute
     * @param aggregationMetadata the walker-produced metadata this plan was built from, or
     *        {@code null} for {@link Type#HITS} plans. Carried through to the response builder
     *        so granularity matching uses the walker's exact nesting-order group fields instead
     *        of re-deriving them from the plan (which loses nesting order).
     */
    public record QueryPlan(Type type, RelNode relNode, AggregationMetadata aggregationMetadata) {
        /**
         * Creates a query plan.
         *
         * @param type what part of the response this plan produces
         * @param relNode the Calcite logical plan to execute
         * @param aggregationMetadata the source metadata for AGGREGATION plans, or null
         */
        public QueryPlan {
            Objects.requireNonNull(type, "type must not be null");
            Objects.requireNonNull(relNode, "relNode must not be null");
        }

        /**
         * Creates a query plan without aggregation metadata (HITS plans).
         *
         * @param type what part of the response this plan produces
         * @param relNode the Calcite logical plan to execute
         */
        public QueryPlan(Type type, RelNode relNode) {
            this(type, relNode, null);
        }
    }

    private final List<QueryPlan> plans;

    private QueryPlans(List<QueryPlan> plans) {
        this.plans = List.copyOf(plans);
    }

    /** Returns all plans. */
    public List<QueryPlan> getAll() {
        return plans;
    }

    /**
     * Returns all plans matching the given type.
     *
     * @param type the plan type to look up
     */
    public List<QueryPlan> get(Type type) {
        return plans.stream().filter(p -> p.type() == type).toList();
    }

    /**
     * Returns true if a plan with the given type exists.
     *
     * @param type the plan type to check
     */
    public boolean has(Type type) {
        return plans.stream().anyMatch(p -> p.type() == type);
    }

    /** Builder for constructing {@link QueryPlans}. */
    public static class Builder {
        private final List<QueryPlan> plans = new ArrayList<>();

        /** Creates a new empty builder. */
        public Builder() {}

        /**
         * Adds a plan.
         *
         * @param plan the plan to add
         */
        public Builder add(QueryPlan plan) {
            plans.add(plan);
            return this;
        }

        /** Builds the plans */
        public QueryPlans build() {
            return new QueryPlans(plans);
        }
    }
}
