/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.deciders;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;

/**
 * Decision class for intra-segment search. Determines whether a query/aggregation
 * benefits from intra-segment parallelization.
 */
@ExperimentalApi
public class IntraSegmentSearchDecision {

    private final DecisionStatus decisionStatus;
    private final String decisionReason;

    public IntraSegmentSearchDecision(DecisionStatus decisionStatus, String decisionReason) {
        this.decisionStatus = decisionStatus;
        this.decisionReason = decisionReason;
    }

    public DecisionStatus getDecisionStatus() {
        return decisionStatus;
    }

    public String getDecisionReason() {
        return decisionReason;
    }

    /**
     * Status indicating whether intra-segment search should be used.
     */
    @ExperimentalApi
    public enum DecisionStatus {
        YES,    // Use intra-segment search
        NO,     // don't use intra-segment search
        NO_OP   // no preference
    }

    @Override
    public String toString() {
        return "IntraSegmentSearchDecision{" + "decisionStatus=" + decisionStatus + ", decisionReason='" + decisionReason + '\'' + '}';
    }

    /**
     * Combines query and aggregation decisions for intra-segment search.
     * Strategy: Aggregations typically dominate latency, so if aggregations benefit,
     * enable intra-segment even if query might regress slightly.
     */
    public static IntraSegmentSearchDecision getCompositeDecision(
        IntraSegmentSearchDecision queryDecision,
        IntraSegmentSearchDecision aggDecision,
        boolean hasAggregations
    ) {
        // If aggregations present and support intra-segment, prioritize aggregation benefit
        if (hasAggregations && aggDecision != null && aggDecision.decisionStatus == DecisionStatus.YES) {
            return new IntraSegmentSearchDecision(DecisionStatus.YES, "aggregations benefit from intra-segment search");
        }
        // If aggregations explicitly say NO, respect that
        if (hasAggregations && aggDecision != null && aggDecision.decisionStatus == DecisionStatus.NO) {
            return new IntraSegmentSearchDecision(DecisionStatus.NO, aggDecision.decisionReason);
        }
        // No aggregations or NO_OP - use query decision
        if (queryDecision != null && queryDecision.decisionStatus == DecisionStatus.NO) {
            return new IntraSegmentSearchDecision(DecisionStatus.NO, queryDecision.decisionReason);
        }
        if (queryDecision != null && queryDecision.decisionStatus == DecisionStatus.YES) {
            return new IntraSegmentSearchDecision(DecisionStatus.YES, queryDecision.decisionReason);
        }
        return new IntraSegmentSearchDecision(DecisionStatus.NO_OP, "no preference");
    }

    /**
     * Combines multiple decisions using pessimistic strategy (any NO = NO).
     */
    public static IntraSegmentSearchDecision getCompositeDecision(Collection<IntraSegmentSearchDecision> allDecisions) {
        DecisionStatus finalStatus = DecisionStatus.NO_OP;
        for (IntraSegmentSearchDecision decision : allDecisions) {
            if (decision.decisionStatus == DecisionStatus.NO) {
                return new IntraSegmentSearchDecision(DecisionStatus.NO, decision.decisionReason);
            }
            if (decision.decisionStatus == DecisionStatus.YES) {
                finalStatus = DecisionStatus.YES;
            }
        }
        return new IntraSegmentSearchDecision(finalStatus, "composite decision result");
    }
}
