/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * A stage in the query DAG. Each stage holds a marked plan fragment (annotations
 * intact, multiple viableBackends per operator/expression) and references to
 * child stages.
 *
 * <p>After plan forking, {@code planAlternatives} contains resolved variants
 * where every viableBackends is narrowed to exactly one backend.
 *
 * @opensearch.internal
 */
public class Stage {

    private final int stageId;
    private final RelNode fragment;
    private final List<Stage> childStages;
    private final ExchangeInfo exchangeInfo;
    private List<StagePlan> planAlternatives;

    public Stage(int stageId, RelNode fragment, List<Stage> childStages, ExchangeInfo exchangeInfo) {
        this.stageId = stageId;
        this.fragment = fragment;
        this.childStages = List.copyOf(childStages);
        this.exchangeInfo = exchangeInfo;
        this.planAlternatives = List.of();
    }

    public int getStageId() {
        return stageId;
    }

    /** Marked plan fragment with annotations intact. */
    public RelNode getFragment() {
        return fragment;
    }

    public List<Stage> getChildStages() {
        return childStages;
    }

    /** How this stage connects to its parent. Null for the root stage. */
    public ExchangeInfo getExchangeInfo() {
        return exchangeInfo;
    }

    public List<StagePlan> getPlanAlternatives() {
        return planAlternatives;
    }

    public void setPlanAlternatives(List<StagePlan> planAlternatives) {
        this.planAlternatives = planAlternatives;
    }
}
