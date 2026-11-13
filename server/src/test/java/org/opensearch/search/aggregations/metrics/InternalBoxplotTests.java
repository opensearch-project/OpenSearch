/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.DoubleSupplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

public class InternalBoxplotTests extends InternalAggregationTestCase<InternalBoxplot> {

    private static final List<String> STAT_NAMES = List.of("min", "max", "q1", "q2", "q3", "lower", "upper");

    @Override
    protected InternalBoxplot createTestInstance(String name, Map<String, Object> metadata) {
        final TDigestState state = new TDigestState(randomDoubleBetween(20, 1000, true));
        final int numberOfValues = frequently() ? randomIntBetween(0, 1000) : 0;
        for (int i = 0; i < numberOfValues; i++) {
            state.add(randomDoubleBetween(-1000, 1000, true));
        }
        return new InternalBoxplot(name, metadata, randomNumericDocValueFormat(), state);
    }

    @Override
    protected void assertReduced(InternalBoxplot reduced, List<InternalBoxplot> inputs) {
        long totalCount = 0;
        double expectedMin = Double.POSITIVE_INFINITY;
        double expectedMax = Double.NEGATIVE_INFINITY;
        for (InternalBoxplot input : inputs) {
            totalCount += input.getState().size();
            if (input.getState().size() > 0) {
                expectedMin = Math.min(expectedMin, input.getMin());
                expectedMax = Math.max(expectedMax, input.getMax());
            }
        }

        assertEquals(totalCount, reduced.getState().size());
        if (totalCount > 0) {
            // min and max are exact, so no delta is allowed
            assertEquals(expectedMin, reduced.getMin(), 0d);
            assertEquals(expectedMax, reduced.getMax(), 0d);
        }
    }

    @Override
    protected void assertFromXContent(InternalBoxplot internalBoxplot, ParsedAggregation parsedAggregation) throws IOException {
        assertTrue(parsedAggregation instanceof ParsedBoxplot);
        ParsedBoxplot parsedBoxplot = (ParsedBoxplot) parsedAggregation;
        // Double.compare handles NaN, which we use for no result
        assertEquals(internalBoxplot.getMin(), parsedBoxplot.getMin(), 0);
        assertEquals(internalBoxplot.getMax(), parsedBoxplot.getMax(), 0);
        assertEquals(internalBoxplot.getQ1(), parsedBoxplot.getQ1(), 0);
        assertEquals(internalBoxplot.getQ2(), parsedBoxplot.getQ2(), 0);
        assertEquals(internalBoxplot.getQ3(), parsedBoxplot.getQ3(), 0);
        assertEquals(internalBoxplot.getLower(), parsedBoxplot.getLower(), 0);
        assertEquals(internalBoxplot.getUpper(), parsedBoxplot.getUpper(), 0);
        if (internalBoxplot.getState().size() > 0) {
            assertEquals(internalBoxplot.getMinAsString(), parsedBoxplot.getMinAsString());
            assertEquals(internalBoxplot.getMaxAsString(), parsedBoxplot.getMaxAsString());
            assertEquals(internalBoxplot.getQ1AsString(), parsedBoxplot.getQ1AsString());
            assertEquals(internalBoxplot.getQ2AsString(), parsedBoxplot.getQ2AsString());
            assertEquals(internalBoxplot.getQ3AsString(), parsedBoxplot.getQ3AsString());
            assertEquals(internalBoxplot.getLowerAsString(), parsedBoxplot.getLowerAsString());
            assertEquals(internalBoxplot.getUpperAsString(), parsedBoxplot.getUpperAsString());
        }
    }

    @Override
    protected InternalBoxplot mutateInstance(InternalBoxplot instance) throws IOException {
        String name = instance.getName();
        TDigestState state = instance.getState();
        Map<String, Object> metadata = instance.getMetadata();

        switch (between(0, 2)) {
            case 0:
                name += randomAlphaOfLengthBetween(2, 10);
                break;
            case 1:
                final TDigestState newState = new TDigestState(instance.getState().compression());
                final int numberOfValues = between(10, 100);
                for (int i = 0; i < numberOfValues; i++) {
                    newState.add(randomDoubleBetween(-1000, 1000, true));
                }
                state = newState;
                break;
            case 2:
                if (metadata == null) {
                    metadata = new HashMap<>(1);
                } else {
                    metadata = new HashMap<>(metadata);
                }
                metadata.put(randomAlphaOfLengthBetween(2, 10), randomInt());
                break;
        }

        return new InternalBoxplot(name, metadata, instance.getFormat(), state);
    }

    public void testSingleValue() {
        final TDigestState state = new TDigestState(100);
        state.add(42.0);

        final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state);

        assertAllStatsEqual(42.0, boxplot);
    }

    public void testIdenticalValues() {
        final TDigestState state = new TDigestState(100);
        for (int i = 0; i < 50; i++) {
            state.add(7.5);
        }

        final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state);

        assertAllStatsEqual(7.5, boxplot);
    }

    public void testWhiskersExcludeOutlier() {
        final TDigestState state = new TDigestState(100);
        for (int i = 1; i <= 100; i++) {
            state.add(i);
        }
        state.add(1000.0);

        final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state);

        assertEquals(1.0, boxplot.getMin(), 0d);
        assertEquals(1000.0, boxplot.getMax(), 0d);
        // the lower whisker is the smallest value, the upper whisker stops before the outlier
        assertEquals(1.0, boxplot.getLower(), 0d);
        assertEquals(100.0, boxplot.getUpper(), 1.0);
        assertEquals(51.0, boxplot.getQ2(), 1.0);
    }

    public void testWhiskersExcludeLowOutlier() {
        final TDigestState state = new TDigestState(100);
        state.add(-1000.0);
        for (int i = 1; i <= 100; i++) {
            state.add(i);
        }

        final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state);

        assertEquals(-1000.0, boxplot.getMin(), 0d);
        assertEquals(100.0, boxplot.getMax(), 0d);
        // the lower whisker stops before the outlier, the upper whisker is the largest value
        assertEquals(1.0, boxplot.getLower(), 1.0);
        assertEquals(100.0, boxplot.getUpper(), 0d);
    }

    public void testZeroIqrWithOutliers() {
        final TDigestState state = new TDigestState(100);
        state.add(-500.0);
        for (int i = 0; i < 20; i++) {
            state.add(5.0);
        }
        state.add(500.0);

        final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state);

        assertEquals(-500.0, boxplot.getMin(), 0d);
        assertEquals(500.0, boxplot.getMax(), 0d);
        // the interquartile range is zero, so both whiskers collapse onto the quartiles
        assertEquals(5.0, boxplot.getQ1(), 0d);
        assertEquals(5.0, boxplot.getQ2(), 0d);
        assertEquals(5.0, boxplot.getQ3(), 0d);
        assertEquals(5.0, boxplot.getLower(), 0d);
        assertEquals(5.0, boxplot.getUpper(), 0d);
    }

    public void testWhiskersOnDenseSkewedData() {
        final double mu = 4;
        final double sigma = 1;
        // a skewed distribution, like latencies, has dense data at its high fence, and a symmetric one has it at both fences
        assertWhiskersCloseToExact("lognormal", () -> Math.exp(mu + sigma * random().nextGaussian()));
        assertWhiskersCloseToExact("normal", () -> mu + sigma * random().nextGaussian());
    }

    /**
     * Checks the whiskers of many values drawn from the given distribution against the exact whiskers. The fences are set from the
     * quartiles of the digest itself, so that only the whisker step is compared and not the error of the quartiles.
     */
    private static void assertWhiskersCloseToExact(String distributionName, DoubleSupplier distribution) {
        final int numberOfValues = 100_000;
        final double[] values = new double[numberOfValues];
        final TDigestState state = new TDigestState(100);
        for (int i = 0; i < numberOfValues; i++) {
            values[i] = distribution.getAsDouble();
            state.add(values[i]);
        }
        Arrays.sort(values);

        final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state);

        final double iqr = boxplot.getQ3() - boxplot.getQ1();
        final double lowFence = boxplot.getQ1() - 1.5 * iqr;
        final double highFence = boxplot.getQ3() + 1.5 * iqr;
        double exactLower = Double.NaN;
        for (int i = 0; i < numberOfValues && Double.isNaN(exactLower); i++) {
            if (values[i] >= lowFence) {
                exactLower = values[i];
            }
        }
        double exactUpper = Double.NaN;
        for (int i = numberOfValues - 1; i >= 0 && Double.isNaN(exactUpper); i--) {
            if (values[i] <= highFence) {
                exactUpper = values[i];
            }
        }

        final double tolerance = 0.02 * iqr;
        assertEquals(distributionName + " lower whisker", exactLower, boxplot.getLower(), tolerance);
        assertEquals(distributionName + " upper whisker", exactUpper, boxplot.getUpper(), tolerance);
    }

    public void testOrderingInvariantRandom() {
        for (int iteration = 0; iteration < 20; iteration++) {
            final TDigestState state = new TDigestState(randomDoubleBetween(20, 500, true));
            final int numberOfValues = randomIntBetween(1, 500);
            for (int i = 0; i < numberOfValues; i++) {
                state.add(randomDoubleBetween(-1e6, 1e6, true));
            }

            final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state);

            assertTrue("min <= lower: " + boxplot.getMin() + " <= " + boxplot.getLower(), boxplot.getMin() <= boxplot.getLower());
            assertTrue("lower <= q1: " + boxplot.getLower() + " <= " + boxplot.getQ1(), boxplot.getLower() <= boxplot.getQ1());
            assertTrue("q1 <= q2: " + boxplot.getQ1() + " <= " + boxplot.getQ2(), boxplot.getQ1() <= boxplot.getQ2());
            assertTrue("q2 <= q3: " + boxplot.getQ2() + " <= " + boxplot.getQ3(), boxplot.getQ2() <= boxplot.getQ3());
            assertTrue("q3 <= upper: " + boxplot.getQ3() + " <= " + boxplot.getUpper(), boxplot.getQ3() <= boxplot.getUpper());
            assertTrue("upper <= max: " + boxplot.getUpper() + " <= " + boxplot.getMax(), boxplot.getUpper() <= boxplot.getMax());
        }
    }

    public void testInfiniteValues() {
        // scripts and missing values can produce infinities, which t-digest accepts
        final TDigestState highOutlier = new TDigestState(100);
        for (int i = 1; i <= 100; i++) {
            highOutlier.add(i);
        }
        highOutlier.add(Double.POSITIVE_INFINITY);
        final InternalBoxplot high = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, highOutlier);
        assertOrderedAndNotNaN(high);
        assertEquals(1.0, high.getMin(), 0d);
        assertEquals(Double.POSITIVE_INFINITY, high.getMax(), 0d);
        assertEquals(100.0, high.getUpper(), 1.0);

        final TDigestState lowOutlier = new TDigestState(100);
        lowOutlier.add(Double.NEGATIVE_INFINITY);
        for (int i = 1; i <= 100; i++) {
            lowOutlier.add(i);
        }
        final InternalBoxplot low = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, lowOutlier);
        assertOrderedAndNotNaN(low);
        assertEquals(Double.NEGATIVE_INFINITY, low.getMin(), 0d);
        assertEquals(100.0, low.getMax(), 0d);
        assertEquals(1.0, low.getLower(), 1.0);

        // every quartile is infinite, so the interquartile range is undefined and the whiskers fall back to min and max
        final TDigestState allInfinite = new TDigestState(100);
        for (int i = 0; i < 10; i++) {
            allInfinite.add(Double.POSITIVE_INFINITY);
        }
        final InternalBoxplot infinite = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, allInfinite);
        assertAllStatsEqual(Double.POSITIVE_INFINITY, infinite);
    }

    private static void assertOrderedAndNotNaN(InternalBoxplot boxplot) {
        for (String stat : STAT_NAMES) {
            assertFalse(stat + " is NaN", Double.isNaN(boxplot.value(stat)));
        }
        assertTrue(boxplot.getMin() <= boxplot.getLower());
        assertTrue(boxplot.getLower() <= boxplot.getQ1());
        assertTrue(boxplot.getQ1() <= boxplot.getQ2());
        assertTrue(boxplot.getQ2() <= boxplot.getQ3());
        assertTrue(boxplot.getQ3() <= boxplot.getUpper());
        assertTrue(boxplot.getUpper() <= boxplot.getMax());
    }

    public void testEmptyRendersNulls() throws IOException {
        final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, new DocValueFormat.Decimal("###.##"), new TDigestState(100));

        final String json = Strings.toString(XContentType.JSON, boxplot);

        final Map<String, Object> body;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> rendered = (Map<String, Object>) parser.map().get("boxplot");
            body = rendered;
        }
        assertEquals(STAT_NAMES.size(), body.size());
        for (String stat : STAT_NAMES) {
            assertTrue("missing [" + stat + "] in " + json, body.containsKey(stat));
            assertNull("[" + stat + "] should be null in " + json, body.get(stat));
        }
        assertThat(json, not(containsString("_as_string")));
    }

    public void testNonEmptyRendersAsStringForNonRawFormat() throws IOException {
        final TDigestState state = new TDigestState(100);
        for (int i = 1; i <= 10; i++) {
            state.add(i);
        }

        final String raw = Strings.toString(XContentType.JSON, new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state));
        final String formatted = Strings.toString(
            XContentType.JSON,
            new InternalBoxplot("boxplot", null, new DocValueFormat.Decimal("###.##"), state)
        );

        assertThat(raw, not(containsString("_as_string")));
        for (String stat : STAT_NAMES) {
            assertThat(formatted, containsString("\"" + stat + "_as_string\""));
        }
    }

    public void testReduceWithEmptyPartial() {
        final TDigestState empty = new TDigestState(100);
        final TDigestState nonEmpty = new TDigestState(100);
        nonEmpty.add(1);
        nonEmpty.add(2);
        nonEmpty.add(3);

        final InternalBoxplot emptyBoxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, empty);
        final InternalBoxplot nonEmptyBoxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, nonEmpty);

        final InternalAggregation reduced = emptyBoxplot.reduce(
            List.of(emptyBoxplot, nonEmptyBoxplot),
            emptyReduceContextBuilder().forFinalReduction()
        );

        assertTrue(reduced instanceof InternalBoxplot);
        final InternalBoxplot reducedBoxplot = (InternalBoxplot) reduced;
        assertEquals(3, reducedBoxplot.getState().size());
        assertArrayEquals(InternalBoxplot.computeStats(nonEmpty), InternalBoxplot.computeStats(reducedBoxplot.getState()), 0d);
        assertEquals(1.0, reducedBoxplot.getMin(), 0d);
        assertEquals(3.0, reducedBoxplot.getMax(), 0d);
    }

    public void testValueByName() {
        final TDigestState state = new TDigestState(100);
        for (int i = 1; i <= 100; i++) {
            state.add(i);
        }

        final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state);

        assertEquals(boxplot.getMin(), boxplot.value("min"), 0d);
        assertEquals(boxplot.getMax(), boxplot.value("max"), 0d);
        assertEquals(boxplot.getQ1(), boxplot.value("q1"), 0d);
        assertEquals(boxplot.getQ2(), boxplot.value("q2"), 0d);
        assertEquals(boxplot.getQ3(), boxplot.value("q3"), 0d);
        assertEquals(boxplot.getLower(), boxplot.value("lower"), 0d);
        assertEquals(boxplot.getUpper(), boxplot.value("upper"), 0d);
    }

    public void testValueByNameUnknown() {
        final TDigestState state = new TDigestState(100);
        state.add(1);
        final InternalBoxplot boxplot = new InternalBoxplot("boxplot", null, DocValueFormat.RAW, state);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> boxplot.value("q4"));

        assertEquals("Unknown value [q4] in boxplot aggregation", e.getMessage());
    }

    private static void assertAllStatsEqual(double expected, InternalBoxplot boxplot) {
        assertEquals(expected, boxplot.getMin(), 0d);
        assertEquals(expected, boxplot.getMax(), 0d);
        assertEquals(expected, boxplot.getQ1(), 0d);
        assertEquals(expected, boxplot.getQ2(), 0d);
        assertEquals(expected, boxplot.getQ3(), 0d);
        assertEquals(expected, boxplot.getLower(), 0d);
        assertEquals(expected, boxplot.getUpper(), 0d);
    }
}
