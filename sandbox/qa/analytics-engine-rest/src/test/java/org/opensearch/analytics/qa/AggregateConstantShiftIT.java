/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * End-to-end results coverage of {@code OpenSearchAggregateConstantShiftRule} over PPL: {@code SUM / COUNT / MIN / MAX
 * (x ± k)} over an integer column is rewritten to arithmetic on {@code fn(x)}. One JUnit case per query in
 * {@link #SUPPORTED_QUERIES} and {@link #UNSUPPORTED_QUERIES}:
 *
 * <ul>
 *   <li>{@link #SUPPORTED_QUERIES}: the rows equal those of the same query with every shift literal written as a
 *       decimal ({@code x + 1} → {@code x + 1.0}), which the integer-only rule leaves alone — a rule-off reference
 *       computed by the same engine on the same data.
 *   <li>{@link #UNSUPPORTED_QUERIES}: the query still executes and returns rows.
 * </ul>
 *
 * <p>Which side of the aggregate each argument lands on is asserted structurally where the planner can be run on the
 * shape directly: {@code AggregatePlanShapeTests} (through {@code PlannerImpl}), the decline categories in
 * {@code OpenSearchAggregateConstantShiftRuleTests}, and the ClickBench q30 plan golden.
 *
 * <p>The query text names the dataset ({@code source = clickbench}); the test points it at its own index so the
 * shard count is fixed here rather than shared with other ITs. Column types: {@code ResolutionWidth} /
 * {@code ResolutionHeight} / {@code OS} are short, {@code RegionID} is integer, {@code ParamPrice} / {@code UserID} /
 * {@code WatchID} are long, {@code SearchPhrase} is keyword, {@code EventTime} is date.
 */
public class AggregateConstantShiftIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset(ClickBenchTestHelper.DATASET.name, "parquet_hits_constant_shift");
    private static final int NUMBER_OF_SHARDS = 2;

    /** The rule rewrites every {@code x ± k} aggregate argument in these queries to run once on the aggregated value. */
    static final List<String> SUPPORTED_QUERIES = List.of(
        // one shifted call per supported function, both signs, literal on either side, large offset
        "source = clickbench | stats sum(ResolutionWidth + 1)",
        "source = clickbench | stats sum(ResolutionWidth - 1)",
        "source = clickbench | stats sum(1 + ResolutionWidth)",
        "source = clickbench | stats sum(ResolutionWidth + 1000000000000)",
        "source = clickbench | stats count(ResolutionWidth + 1)",
        "source = clickbench | stats min(ResolutionWidth + 1)",
        "source = clickbench | stats max(ResolutionWidth - 2)",
        // AVG and VAR are reduced to SUM / COUNT first and collapse in the same rule collection
        "source = clickbench | stats avg(ResolutionWidth + 1)",
        "source = clickbench | stats var_samp(ResolutionWidth + 1)",
        // terms over one column share one set of accumulators
        "source = clickbench | stats sum(ResolutionWidth), sum(ResolutionWidth + 1)",
        "source = clickbench | stats sum(ResolutionWidth + 1), sum(ResolutionWidth + 2), sum(ResolutionWidth + 3)",
        "source = clickbench | stats sum(ResolutionWidth + 1), sum(ResolutionWidth - 1)",
        "source = clickbench | stats sum(OS + 1), count(OS + 2), min(OS + 3), max(OS + 4)",
        "source = clickbench | stats sum(ResolutionWidth + 1), sum(ResolutionHeight + 1)",
        "source = clickbench | stats sum(ResolutionWidth + 1) as a, sum(ResolutionWidth + 1) as b",
        "source = clickbench | stats count(ResolutionWidth + 1), count(ResolutionWidth)",
        // calls that do not take part are carried along unchanged
        "source = clickbench | stats sum(ResolutionWidth + 1), count()",
        "source = clickbench | stats sum(ResolutionWidth + 1), var_samp(ResolutionWidth)",
        "source = clickbench | stats sum(ResolutionWidth + 1), distinct_count(RegionID)",
        "source = clickbench | stats sum(ResolutionWidth + 1), sum(ResolutionWidth * 2)",
        "source = clickbench | stats sum(ResolutionWidth + 1), sum(ResolutionWidth + ResolutionHeight)",
        // the column can be any deterministic integer expression; nested shifts collapse through the fixpoint
        "source = clickbench | stats sum(ResolutionWidth + 1 + 2)",
        "source = clickbench | stats sum(abs(ResolutionWidth) + 1)",
        "source = clickbench | stats sum(length(SearchPhrase) + 1)",
        "source = clickbench | stats sum(RegionID * ResolutionWidth + 1)",
        "source = clickbench | eval w = ResolutionWidth * 2 | stats sum(w + 1)",
        "source = clickbench | eval w = ResolutionWidth + 1 | stats sum(w)",
        "source = clickbench | eval w = ResolutionWidth + 1 | where w > 1000 | stats sum(w)",
        "source = clickbench | eval n = case(RegionID > 200, ResolutionWidth) | stats sum(n + 1), count(n + 1)",
        // lossless casts of one column are one column; integer types other than short
        "source = clickbench | stats sum(cast(ResolutionWidth as long) + 1)",
        "source = clickbench | stats sum(ResolutionWidth + 1), sum(cast(ResolutionWidth as long) + 2)",
        "source = clickbench | stats sum(RegionID + 1), min(RegionID + 1), max(RegionID + 1)",
        "source = clickbench | stats sum(ParamPrice + 1)",
        "source = clickbench | stats min(ResolutionWidth + 1000000000000), max(ResolutionWidth - 1000000000000)",
        // group keys
        "source = clickbench | stats sum(ResolutionWidth + 1) by RegionID",
        "source = clickbench | stats sum(ResolutionWidth + 1) by RegionID, OS",
        "source = clickbench | stats sum(ResolutionWidth + 1) by ResolutionWidth",
        "source = clickbench | stats sum(ResolutionWidth + 1) by span(EventTime, 1d)",
        // operators around the aggregate
        "source = clickbench | where ResolutionWidth + 1 > 1000 | stats sum(ResolutionWidth + 1)",
        "source = clickbench | stats sum(ResolutionWidth + 1) as s by RegionID | sort - s, RegionID | head 5",
        "source = clickbench | stats sum(ResolutionWidth + 1) as s by RegionID | where s > 1000",
        "source = clickbench | stats sum(ResolutionWidth + 1) as s by RegionID | stats sum(s + 1)",
        "source = clickbench | stats sum(ResolutionWidth + 1) as s | eval t = s + 1",
        "source = clickbench | join left = l right = r on l.RegionID = r.RegionID clickbench | stats sum(l.ResolutionWidth + 1)",
        "source = clickbench | append [ source = clickbench ] | stats sum(ResolutionWidth + 1)",
        "source = clickbench | where RegionID in [ source = clickbench | where OS > 0 | fields RegionID ] | stats sum(ResolutionWidth + 1)"
    );

    /** The rule declines these: every aggregate argument stays the per-row expression the query wrote. */
    static final List<String> UNSUPPORTED_QUERIES = List.of(
        // not x ± k: other operators, literal on the left of a minus
        "source = clickbench | stats sum(ResolutionWidth * 2)",
        "source = clickbench | stats sum(ResolutionWidth / 2)",
        "source = clickbench | stats sum(ResolutionWidth % 7)",
        "source = clickbench | stats sum(2 - ResolutionWidth)",
        "source = clickbench | stats sum(-1 - ResolutionWidth)",
        "source = clickbench | stats sum((ResolutionWidth + 1) * 2)",
        // not an integer literal, or not an integer column (floating-point addition is not associative)
        "source = clickbench | stats sum(ResolutionWidth + 1.5)",
        "source = clickbench | stats sum(ResolutionWidth + 1.0)",
        "source = clickbench | stats avg(ResolutionWidth + 1.5)",
        "source = clickbench | stats sum(ResolutionWidth + ResolutionHeight)",
        "source = clickbench | stats sum(cast(ResolutionWidth as double) + 1)",
        "source = clickbench | stats max(cast(ResolutionWidth as double) - 1)",
        "source = clickbench | stats count(cast(ResolutionWidth as double) + 1)",
        "source = clickbench | stats sum(SearchPhrase + 1)",
        // unsupported aggregate functions
        "source = clickbench | stats distinct_count(ResolutionWidth + 1)",
        "source = clickbench | stats percentile(ResolutionWidth + 1, 50)",
        // MIN / MAX only when no row can overflow: never over a BIGINT column or expression, nor for a huge offset
        "source = clickbench | stats min(UserID + 1)",
        "source = clickbench | stats max(WatchID - 1)",
        "source = clickbench | stats min(ResolutionWidth * 2 + 1)",
        "source = clickbench | stats min(ResolutionWidth + 9223372036854770000)",
        // the shift was computed by an eval below another operator: the Project under the aggregate holds a bare column
        "source = clickbench | eval w = ResolutionWidth + 1 | sort w | stats sum(w)",
        "source = clickbench | eval w = ResolutionWidth + 1 | head 100 | stats sum(w)",
        "source = clickbench | eval w = ResolutionWidth + 1 | dedup RegionID | stats sum(w)"
    );

    /** One query and whether the rule rewrites it. {@code toString()} names the JUnit case. */
    public static final class QueryCase {
        final String ppl;
        final boolean supported;

        QueryCase(String ppl, boolean supported) {
            this.ppl = ppl;
            this.supported = supported;
        }

        @Override
        public String toString() {
            return (supported ? "supported: " : "unsupported: ") + ppl;
        }
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        List<Object[]> cases = new ArrayList<>();
        SUPPORTED_QUERIES.forEach(ppl -> cases.add(new Object[] { new QueryCase(ppl, true) }));
        UNSUPPORTED_QUERIES.forEach(ppl -> cases.add(new Object[] { new QueryCase(ppl, false) }));
        return cases;
    }

    private final QueryCase testCase;

    public AggregateConstantShiftIT(@Name("query") QueryCase testCase) {
        this.testCase = testCase;
    }

    private static volatile boolean provisioned = false;

    @Override
    protected void onBeforeQuery() throws IOException {
        if (provisioned == false) {
            DatasetProvisioner.provision(client(), DATASET, NUMBER_OF_SHARDS);
            provisioned = true;
        }
    }

    public void testResults() throws IOException {
        String ppl = testCase.ppl.replace(DATASET.name, DATASET.indexName);
        Map<String, Object> response = executePpl(ppl);
        assertNotNull(ppl + ": no datarows", datarows(response));
        if (testCase.supported == false) {
            return;
        }
        String twin = withDecimalShifts(ppl);
        assertNotEquals("every supported query must carry a shift literal to rewrite: " + ppl, ppl, twin);
        assertSameRows(ppl, datarows(executePpl(twin)), datarows(response));
    }

    // ---- Rule-off reference ----

    /** A shift literal: the integer after {@code +} / {@code -} that ends an argument, or the one that opens {@code (k + x}. */
    private static final Pattern SHIFT_AFTER_OPERATOR = Pattern.compile("([+-]\\s*)(\\d+)(?=\\s*[,)|]|\\s*$)");
    private static final Pattern SHIFT_BEFORE_PLUS = Pattern.compile("(\\(\\s*)(\\d+)(?=\\s*\\+)");

    /**
     * The same query with every shift literal written as a decimal ({@code x + 1} → {@code x + 1.0}). The argument is
     * no longer integer-typed, so the rule leaves it per row; the values are the same numbers, so the rows must match.
     */
    private static String withDecimalShifts(String ppl) {
        String twin = SHIFT_AFTER_OPERATOR.matcher(ppl).replaceAll("$1$2.0");
        return SHIFT_BEFORE_PLUS.matcher(twin).replaceAll("$1$2.0");
    }

    /** Same rows as a multiset (a query without a sort has no row order), numbers compared as numbers. */
    private static void assertSameRows(String ppl, List<List<Object>> expected, List<List<Object>> actual) {
        assertEquals(ppl + ": row count", expected.size(), actual.size());
        List<List<Object>> expectedRows = sortedRows(expected);
        List<List<Object>> actualRows = sortedRows(actual);
        for (int row = 0; row < expectedRows.size(); row++) {
            List<Object> want = expectedRows.get(row);
            List<Object> got = actualRows.get(row);
            assertEquals(ppl + ": column count in row " + row, want.size(), got.size());
            for (int col = 0; col < want.size(); col++) {
                assertSameValue(ppl + ": row " + row + " column " + col, want.get(col), got.get(col));
            }
        }
    }

    private static void assertSameValue(String label, Object expected, Object actual) {
        if (expected instanceof Number want && actual instanceof Number got) {
            double a = want.doubleValue();
            double b = got.doubleValue();
            // Sums, counts, minima and maxima agree exactly; the decimal twin prints avg / variance with five decimals.
            assertEquals(label, a, b, 1e-5 + 1e-9 * Math.max(Math.abs(a), Math.abs(b)));
        } else {
            assertEquals(label, expected, actual);
        }
    }

    private static List<List<Object>> sortedRows(List<List<Object>> rows) {
        return rows.stream().sorted(Comparator.comparing(AggregateConstantShiftIT::rowKey)).collect(Collectors.toList());
    }

    /** Row text with every number in one fixed format so integer and decimal renderings of one value sort alike. */
    private static String rowKey(List<Object> row) {
        return row.stream()
            .map(cell -> cell instanceof Number n ? String.format(Locale.ROOT, "%.4f", n.doubleValue()) : String.valueOf(cell))
            .collect(Collectors.joining("|"));
    }

    @SuppressWarnings("unchecked")
    private static List<List<Object>> datarows(Map<String, Object> response) {
        return (List<List<Object>>) response.get("datarows");
    }
}
