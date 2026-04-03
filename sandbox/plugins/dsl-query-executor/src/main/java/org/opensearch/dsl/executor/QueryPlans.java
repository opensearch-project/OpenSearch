/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.executor;

import org.apache.calcite.rel.RelNode;

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
        AGGREGATION
    }

    /**
     * A single plan pairing a {@link Type} with a Calcite {@link RelNode}.
     *
     * @param type what part of the response this plan produces
     * @param relNode the Calcite logical plan to execute
     */
    // TODO: Nested aggregations may require multiple RelNodes per aggregation.
    // Support linking child query plans for recursive nesting (e.g. nested sub-aggregations).
    public record QueryPlan(Type type, RelNode relNode) {
        /**
         * Creates a query plan.
         *
         * @param type what part of the response this plan produces
         * @param relNode the Calcite logical plan to execute
         */
        public QueryPlan {
            Objects.requireNonNull(type, "type must not be null");
            Objects.requireNonNull(relNode, "relNode must not be null");
        }

        /** Returns what part of the response this plan produces. */
        @Override
        public Type type() {
            return type;
        }

        /** Returns the Calcite logical plan to execute. */
        @Override
        public RelNode relNode() {
            return relNode;
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

        /** Builds the plans. May be empty (e.g. size=0, no aggs — only metadata response). */
        public QueryPlans build() {
            return new QueryPlans(plans);
        }
    }
}
