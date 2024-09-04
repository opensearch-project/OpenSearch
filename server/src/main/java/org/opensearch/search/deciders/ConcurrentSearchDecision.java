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
 * This Class defines the decisions that a {@link ConcurrentSearchDecider#getConcurrentSearchDecision} can return.
 *
 */
@ExperimentalApi
public class ConcurrentSearchDecision {

    final private DecisionStatus decisionStatus;
    final private String decisionReason;

    public ConcurrentSearchDecision(DecisionStatus decisionStatus, String decisionReason) {
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
     * This enum contains the decision status for concurrent search.
     */
    @ExperimentalApi
    public enum DecisionStatus {
        YES(0), // use concurrent search
        NO(1),  // don't use concurrent search
        NO_OP(2); // no preference

        private final int id;

        DecisionStatus(int id) {
            this.id = id;
        }
    }

    @Override
    public String toString() {
        return "ConcurrentSearchDecision{" + "decisionStatus=" + decisionStatus + ", decisionReason='" + decisionReason + '\'' + '}';
    }

    /**
     * Combine a collection of {@link ConcurrentSearchDecision} to return final {@link ConcurrentSearchDecision}
     * The decisions are combined as:
     * NO_OP AND NO_OP results in NO_OP
     * NO_OP AND YES results in YES
     * NO_OP AND NO results in NO
     */
    public static ConcurrentSearchDecision getCompositeDecision(Collection<ConcurrentSearchDecision> allDecisions) {

        DecisionStatus finalDecisionStatus = DecisionStatus.NO_OP;
        for (ConcurrentSearchDecision decision : allDecisions) {
            switch (decision.decisionStatus) {
                case YES:
                    finalDecisionStatus = DecisionStatus.YES;
                    break;
                case NO:
                    finalDecisionStatus = DecisionStatus.NO;
                    return new ConcurrentSearchDecision(
                        finalDecisionStatus,
                        "composite decision evaluated to false due to " + decision.decisionReason
                    );
                case NO_OP:
                    // NOOP doesn't change the final decision
                    break;
            }
        }
        return new ConcurrentSearchDecision(finalDecisionStatus, "composite decision result");
    }

}
