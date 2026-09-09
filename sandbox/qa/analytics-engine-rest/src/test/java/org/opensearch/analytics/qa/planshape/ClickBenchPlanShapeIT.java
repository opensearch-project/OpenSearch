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
import java.util.stream.IntStream;

/**
 * Plan-shape golden IT for ClickBench. Each (query, combo) is reported as its own JUnit case named
 * {@code clickbench/q{N}[combo]}; target one with {@code --tests "*ClickBenchPlanShapeIT*q8*"}.
 *
 * <p>All harness logic is in {@link PlanShapeGoldenTestBase}; this class only supplies the
 * {@link WorkloadSpec}. The {@code @ParametersFactory} must be static, so it cannot read an
 * instance field — hence the static {@link #SPEC}.
 *
 * <p>All 43 ClickBench PPL queries are exercised. Each query's golden declares which combos it
 * applies to — queries whose shard physical plan is absent (lucene fast-path) simply omit that layer.
 */
public class ClickBenchPlanShapeIT extends PlanShapeGoldenTestBase {

    private static final WorkloadSpec SPEC = new WorkloadSpec(
        "clickbench",
        ClickBenchTestHelper.DATASET,
        "datasets/clickbench/ppl",
        "planshape/clickbench",
        IntStream.rangeClosed(1, 43).mapToObj(i -> "q" + i).toList()
    );

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return buildQueryAndSettingCombinationsToRun(SPEC);
    }

    public ClickBenchPlanShapeIT(@Name("case") GoldenCase testCase) {
        super(testCase);
    }
}
