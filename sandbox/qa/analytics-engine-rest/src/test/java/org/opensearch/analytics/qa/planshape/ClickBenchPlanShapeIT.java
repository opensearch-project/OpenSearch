/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.planshape;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.analytics.qa.ClickBenchTestHelper;

import java.util.List;

/**
 * Plan-shape golden IT for ClickBench. Each (query, combo) is reported as its own JUnit case named
 * {@code clickbench/q{N}[combo]}; target one with {@code --tests "*ClickBenchPlanShapeIT*q8*"}.
 *
 * <p>All harness logic is in {@link PlanShapeGoldenTestBase}; this class only supplies the
 * {@link WorkloadSpec}. The {@code @ParametersFactory} must be static, so it cannot read an
 * instance field — hence the static {@link #SPEC}.
 *
 * <p>Query allowlist: ClickBench queries that return rows on the shipped 100-doc dataset (others
 * match zero rows and produce no shard physical plan). Excluded queries are tracked for a future
 * richer dataset.
 */
public class ClickBenchPlanShapeIT extends PlanShapeGoldenTestBase {

    private static final WorkloadSpec SPEC = new WorkloadSpec(
        "clickbench",
        ClickBenchTestHelper.DATASET,
        "datasets/clickbench/ppl",
        "planshape/clickbench",
        // FIXME [RemoveBeforeMerge]: bring-up allowlist = only queries with an authored golden.
        // Full rows>0 allowlist (regenerate goldens then restore):
        //   q1 q2 q3 q4 q5 q6 q7 q8 q9 q10 q16 q17 q18 q19 q21 q30 q33 q34 q35 q36
        List.of("q1", "q5", "q8", "q16", "q21")
    );

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return buildQueryAndSettingCombinationsToRun(SPEC);
    }

    public ClickBenchPlanShapeIT(@Name("case") GoldenCase testCase) {
        super(testCase);
    }
}
