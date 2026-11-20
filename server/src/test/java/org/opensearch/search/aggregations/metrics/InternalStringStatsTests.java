/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.Version;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.aggregations.ParsedAggregation;
import org.opensearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Predicate;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class InternalStringStatsTests extends InternalAggregationTestCase<InternalStringStats> {

    private static final double LOG_2 = Math.log(2);

    private static final String[] DRAFT_EXAMPLE = { "google.com", "x7k2qz9vh3.net", "mail.google.com" };

    /**
     * Builds the result of the given values the slow and obvious way, with the JDK's code point iteration, so that the tests do not
     * depend on the code under test to know what the right answer is.
     */
    private static InternalStringStats fromValues(String name, boolean showDistribution, String... values) {
        return fromValues(name, null, showDistribution, values);
    }

    private static InternalStringStats fromValues(String name, Map<String, Object> metadata, boolean showDistribution, String... values) {
        final Map<Integer, Long> frequencies = new TreeMap<>();
        int min = Integer.MAX_VALUE;
        int max = 0;
        for (String value : values) {
            final int length = value.codePointCount(0, value.length());
            min = Math.min(min, length);
            max = Math.max(max, length);
            value.codePoints().forEach(codePoint -> frequencies.merge(codePoint, 1L, Long::sum));
        }
        final int[] codePoints = new int[frequencies.size()];
        final long[] counts = new long[frequencies.size()];
        int next = 0;
        for (Map.Entry<Integer, Long> entry : frequencies.entrySet()) {
            codePoints[next] = entry.getKey();
            counts[next++] = entry.getValue();
        }
        return new InternalStringStats(
            name,
            metadata,
            values.length,
            values.length == 0 ? 0 : min,
            max,
            codePoints,
            counts,
            showDistribution
        );
    }

    private static double entropyOf(Map<Integer, Long> frequencies) {
        long total = 0;
        for (long count : frequencies.values()) {
            total += count;
        }
        double entropy = 0;
        for (long count : frequencies.values()) {
            final double probability = (double) count / total;
            entropy -= probability * Math.log(probability) / LOG_2;
        }
        return entropy;
    }

    private static Map<Integer, Long> frequenciesOf(InternalStringStats stats) {
        final Map<Integer, Long> frequencies = new TreeMap<>();
        for (int i = 0; i < stats.codePoints().length; i++) {
            frequencies.put(stats.codePoints()[i], stats.characterCounts()[i]);
        }
        return frequencies;
    }

    @Override
    protected InternalStringStats createTestInstance(String name, Map<String, Object> metadata) {
        final String[] values = new String[frequently() ? randomIntBetween(0, 25) : 0];
        for (int i = 0; i < values.length; i++) {
            final String value = randomBoolean()
                ? randomAlphaOfLengthBetween(0, 20)
                : randomRealisticUnicodeOfCodepointLengthBetween(0, 20);
            // The keys of the distribution are characters, and these results are written in a random format. Of all the code points
            // there is one that the YAML writer writes into a key as it is, and so makes the document unreadable: U+0085, next line.
            // JSON, SMILE and CBOR have no such character, which testTheNextLineCharacterInTheDistribution looks at for JSON.
            values[i] = value.replace('\u0085', ' ');
        }
        return fromValues(name, metadata, randomBoolean(), values);
    }

    @Override
    protected void assertReduced(InternalStringStats reduced, List<InternalStringStats> inputs) {
        long expectedCount = 0;
        int expectedMin = Integer.MAX_VALUE;
        int expectedMax = 0;
        final Map<Integer, Long> expectedFrequencies = new TreeMap<>();
        for (InternalStringStats input : inputs) {
            expectedCount += input.getCount();
            if (input.getCount() > 0) {
                expectedMin = Math.min(expectedMin, (int) input.getMinLength());
                expectedMax = Math.max(expectedMax, (int) input.getMaxLength());
            }
            frequenciesOf(input).forEach((codePoint, count) -> expectedFrequencies.merge(codePoint, count, Long::sum));
        }

        assertEquals(expectedCount, reduced.getCount());
        assertEquals(expectedFrequencies, frequenciesOf(reduced));
        if (expectedCount == 0) {
            assertTrue(Double.isNaN(reduced.getMinLength()));
            assertTrue(Double.isNaN(reduced.getMaxLength()));
            assertTrue(Double.isNaN(reduced.getAvgLength()));
            assertTrue(Double.isNaN(reduced.getEntropy()));
        } else {
            assertEquals(expectedMin, reduced.getMinLength(), 0d);
            assertEquals(expectedMax, reduced.getMaxLength(), 0d);
            final long totalCharacters = expectedFrequencies.values().stream().mapToLong(Long::longValue).sum();
            assertEquals((double) totalCharacters / expectedCount, reduced.getAvgLength(), 1e-9);
            // the entropy is that of the merged character counts, not some combination of the entropies of the inputs
            assertEquals(entropyOf(expectedFrequencies), reduced.getEntropy(), 1e-9);
        }
    }

    @Override
    protected void assertFromXContent(InternalStringStats stats, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedStringStats);
        final ParsedStringStats parsed = (ParsedStringStats) parsedAggregation;
        assertEquals(stats.getCount(), parsed.getCount());
        // assertEquals with a delta treats two NaN as equal, which is what a bucket without values has
        assertEquals(stats.getMinLength(), parsed.getMinLength(), 0d);
        assertEquals(stats.getMaxLength(), parsed.getMaxLength(), 0d);
        assertEquals(stats.getAvgLength(), parsed.getAvgLength(), 0d);
        assertEquals(stats.getEntropy(), parsed.getEntropy(), 0d);
        assertEquals(stats.getDistribution(), parsed.getDistribution());
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        // the keys of the distribution are the characters of the values, so there is no such thing as an unknown field in it
        return path -> path.contains(InternalStringStats.DISTRIBUTION_FIELD);
    }

    @Override
    protected InternalStringStats mutateInstance(InternalStringStats instance) {
        String name = instance.getName();
        Map<String, Object> metadata = instance.getMetadata();
        long count = instance.getCount();
        boolean showDistribution = instance.showDistribution();
        switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLengthBetween(2, 10);
                break;
            case 1:
                metadata = metadata == null ? new HashMap<>(1) : new HashMap<>(metadata);
                metadata.put(randomAlphaOfLengthBetween(2, 10), randomInt());
                break;
            case 2:
                showDistribution = showDistribution == false;
                break;
            default:
                count += between(1, 100);
                break;
        }
        return new InternalStringStats(
            name,
            metadata,
            count,
            instance.minLength(),
            instance.maxLength(),
            instance.codePoints(),
            instance.characterCounts(),
            showDistribution
        );
    }

    public void testTheExampleOfTheFeatureRequest() {
        final InternalStringStats stats = fromValues("domain_shape", true, DRAFT_EXAMPLE);

        assertEquals(3, stats.getCount());
        assertEquals(10.0, stats.getMinLength(), 0d);
        assertEquals(15.0, stats.getMaxLength(), 0d);
        assertEquals(13.0, stats.getAvgLength(), 0d);
        assertEquals(4.0604, stats.getEntropy(), 0.00005);
        // 6 of the 39 characters are an o, 4 are a g, and 4 are a dot
        assertEquals(6.0 / 39, stats.getDistribution().get("o"), 1e-12);
        assertEquals(4.0 / 39, stats.getDistribution().get("g"), 1e-12);
        assertEquals(4.0 / 39, stats.getDistribution().get(".").doubleValue(), 1e-12);
        assertEquals(21, stats.getDistribution().size());
    }

    public void testRenderingWithoutTheDistribution() throws IOException {
        final InternalStringStats stats = fromValues("domain_shape", false, DRAFT_EXAMPLE);

        final Map<String, Object> rendered = renderBody(stats);

        assertEquals(List.of("count", "min_length", "max_length", "avg_length", "entropy"), new ArrayList<>(rendered.keySet()));
        assertEquals(3, rendered.get("count"));
        assertEquals(10, rendered.get("min_length"));
        assertEquals(15, rendered.get("max_length"));
        assertEquals(13.0, rendered.get("avg_length"));
        assertEquals(4.0604, (double) rendered.get("entropy"), 0.00005);
        // the draft shows the lengths as integers and the average with a decimal point
        final String json = Strings.toString(XContentType.JSON, stats);
        assertThat(json, containsString("\"min_length\":10,"));
        assertThat(json, containsString("\"max_length\":15,"));
        assertThat(json, containsString("\"avg_length\":13.0,"));
    }

    public void testRenderingWithTheDistribution() throws IOException {
        final InternalStringStats stats = fromValues("domain_shape", true, DRAFT_EXAMPLE);

        final Map<String, Object> rendered = renderBody(stats);

        assertEquals(
            List.of("count", "min_length", "max_length", "avg_length", "entropy", "distribution"),
            new ArrayList<>(rendered.keySet())
        );
        @SuppressWarnings("unchecked")
        final Map<String, Object> distribution = (Map<String, Object>) rendered.get("distribution");
        assertEquals(21, distribution.size());
        assertEquals(6.0 / 39, (double) distribution.get("o"), 1e-12);
        double total = 0;
        for (Object frequency : distribution.values()) {
            total += (double) frequency;
        }
        assertEquals(1.0, total, 1e-9);
    }

    public void testDistributionIsOrderedByFrequencyThenCharacter() throws IOException {
        // c occurs three times, b and d twice, and a and e once, with a character from the supplementary planes among the last
        final InternalStringStats stats = fromValues("s", true, "ccc", "bb", "dd", "a", "\uD83D\uDE00");

        @SuppressWarnings("unchecked")
        final Map<String, Object> distribution = (Map<String, Object>) renderBody(stats).get("distribution");

        assertEquals(List.of("c", "b", "d", "a", "\uD83D\uDE00"), new ArrayList<>(distribution.keySet()));
    }

    public void testDistributionKeysAreWholeCharacters() throws IOException {
        // the smiley is one character that takes two UTF-16 units, so its key has to be a string of both
        final InternalStringStats stats = fromValues("s", true, "\uD83D\uDE00\uD83D\uDE00a");

        @SuppressWarnings("unchecked")
        final Map<String, Object> distribution = (Map<String, Object>) renderBody(stats).get("distribution");

        assertEquals(2, distribution.size());
        assertEquals(2.0 / 3, (double) distribution.get("\uD83D\uDE00"), 1e-12);
        assertEquals(1.0 / 3, (double) distribution.get("a"), 1e-12);
        assertEquals(3.0, fromValues("s", false, "\uD83D\uDE00\uD83D\uDE00a").getMaxLength(), 0d);
    }

    public void testControlCharactersAreEscapedInTheDistribution() throws IOException {
        final InternalStringStats stats = fromValues("s", true, "a\u0000\n\"\\");

        final String json = Strings.toString(XContentType.JSON, stats);

        assertThat(json, containsString("\"\\u0000\":0.2"));
        assertThat(json, containsString("\"\\n\":0.2"));
        @SuppressWarnings("unchecked")
        final Map<String, Object> distribution = (Map<String, Object>) renderBody(stats).get("distribution");
        assertEquals(Set.of("a", "\u0000", "\n", "\"", "\\"), distribution.keySet());
    }

    public void testTheNextLineCharacterInTheDistribution() throws IOException {
        final InternalStringStats stats = fromValues("s", true, "a\u0085");

        @SuppressWarnings("unchecked")
        final Map<String, Object> distribution = (Map<String, Object>) renderBody(stats).get("distribution");

        assertEquals(Set.of("a", "\u0085"), distribution.keySet());
    }

    public void testEmptyResult() throws IOException {
        final InternalStringStats stats = fromValues("s", false);

        assertEquals(0, stats.getCount());
        assertEquals(0.0, stats.value("count"), 0d);
        for (String metric : List.of("min_length", "max_length", "avg_length", "entropy")) {
            assertTrue(metric + " has no value without values", Double.isNaN(stats.value(metric)));
        }
        assertTrue(stats.getDistribution().isEmpty());
        assertEquals(
            "{\"s\":{\"count\":0,\"min_length\":null,\"max_length\":null,\"avg_length\":null,\"entropy\":null}}",
            Strings.toString(XContentType.JSON, stats)
        );
    }

    public void testEmptyResultWithTheDistribution() {
        final InternalStringStats stats = fromValues("s", true);

        assertEquals(
            "{\"s\":{\"count\":0,\"min_length\":null,\"max_length\":null,\"avg_length\":null,\"entropy\":null,\"distribution\":null}}",
            Strings.toString(XContentType.JSON, stats)
        );
    }

    public void testSingleDistinctCharacterHasZeroEntropy() {
        final InternalStringStats stats = fromValues("s", false, "aaaa", "aa");

        // exactly positive zero, not negative zero and not a rounding error
        assertEquals(Double.doubleToLongBits(0.0), Double.doubleToLongBits(stats.getEntropy()));
        assertEquals(1, frequenciesOf(stats).size());
    }

    public void testEqualFrequenciesHaveEntropyLogOfTheAlphabetSize() {
        assertEquals(1.0, fromValues("s", false, "ab").getEntropy(), 1e-12);
        assertEquals(2.0, fromValues("s", false, "abcd", "dcba").getEntropy(), 1e-12);
        assertEquals(3.0, fromValues("s", false, "abcdefgh").getEntropy(), 1e-12);
    }

    public void testValuesWithoutCharacters() throws IOException {
        final InternalStringStats stats = fromValues("s", true, "", "");

        assertEquals(2, stats.getCount());
        assertEquals(0.0, stats.getMinLength(), 0d);
        assertEquals(0.0, stats.getMaxLength(), 0d);
        assertEquals(0.0, stats.getAvgLength(), 0d);
        assertEquals(Double.doubleToLongBits(0.0), Double.doubleToLongBits(stats.getEntropy()));
        assertTrue(stats.getDistribution().isEmpty());
        assertEquals(
            "{\"s\":{\"count\":2,\"min_length\":0,\"max_length\":0,\"avg_length\":0.0,\"entropy\":0.0,\"distribution\":{}}}",
            Strings.toString(XContentType.JSON, stats)
        );
    }

    public void testDistributionIsEmptyUnlessRequested() {
        assertTrue(fromValues("s", false, "abc").getDistribution().isEmpty());
        assertEquals(3, fromValues("s", true, "abc").getDistribution().size());
    }

    public void testReduceMergesCharacterCountsInsteadOfAveragingEntropies() {
        // either shard sees a single character, so either entropy is zero, yet together the characters are equally frequent
        final InternalStringStats first = fromValues("s", false, "aaaa");
        final InternalStringStats second = fromValues("s", false, "bbbb");
        assertEquals(0.0, first.getEntropy(), 0d);
        assertEquals(0.0, second.getEntropy(), 0d);

        final InternalStringStats reduced = (InternalStringStats) first.reduce(
            List.of(first, second),
            emptyReduceContextBuilder().forFinalReduction()
        );

        assertEquals(2, reduced.getCount());
        assertEquals(1.0, reduced.getEntropy(), 1e-12);
        assertEquals(4.0, reduced.getAvgLength(), 0d);
    }

    public void testReduceOfTheExampleSplitInAnyWayGivesTheSameResult() {
        final InternalStringStats whole = fromValues("s", true, DRAFT_EXAMPLE);
        final InternalStringStats one = fromValues("s", true, DRAFT_EXAMPLE[0]);
        final InternalStringStats two = fromValues("s", true, DRAFT_EXAMPLE[1], DRAFT_EXAMPLE[2]);

        final InternalStringStats forwards = (InternalStringStats) one.reduce(
            List.of(one, two),
            emptyReduceContextBuilder().forFinalReduction()
        );
        final InternalStringStats backwards = (InternalStringStats) two.reduce(
            List.of(two, one),
            emptyReduceContextBuilder().forFinalReduction()
        );

        // the result does not depend on the order the shards answer in, down to the last bit of the entropy
        assertEquals(whole, forwards);
        assertEquals(whole, backwards);
        assertEquals(Double.doubleToLongBits(whole.getEntropy()), Double.doubleToLongBits(forwards.getEntropy()));
        assertEquals(Double.doubleToLongBits(whole.getEntropy()), Double.doubleToLongBits(backwards.getEntropy()));
    }

    public void testReduceIgnoresShardsWithoutValues() {
        final InternalStringStats empty = fromValues("s", false);
        final InternalStringStats filled = fromValues("s", false, "abc", "de");

        final InternalStringStats reduced = (InternalStringStats) empty.reduce(
            List.of(empty, filled, empty),
            emptyReduceContextBuilder().forFinalReduction()
        );

        // an empty shard must not drag the minimum length down to zero
        assertEquals(filled, reduced);
        assertEquals(2.0, reduced.getMinLength(), 0d);
    }

    public void testReduceOfOnlyEmptyResults() {
        final InternalStringStats empty = fromValues("s", true);

        final InternalStringStats reduced = (InternalStringStats) empty.reduce(
            List.of(empty, empty),
            emptyReduceContextBuilder().forFinalReduction()
        );

        assertEquals(empty, reduced);
        assertEquals(0, reduced.getCount());
    }

    public void testReduceKeepsTheLengthsOfTheShortestAndLongestShard() {
        final InternalStringStats shortValues = fromValues("s", false, "ab", "abc");
        final InternalStringStats longValues = fromValues("s", false, "abcdefgh", "abcdef");

        final InternalStringStats reduced = (InternalStringStats) shortValues.reduce(
            List.of(shortValues, longValues),
            emptyReduceContextBuilder().forFinalReduction()
        );

        assertEquals(2.0, reduced.getMinLength(), 0d);
        assertEquals(8.0, reduced.getMaxLength(), 0d);
        assertEquals(19.0 / 4, reduced.getAvgLength(), 0d);
    }

    public void testReducePastTheLimitOfDistinctCharacters() {
        final int half = InternalStringStats.MAX_DISTINCT_CHARACTERS / 2 + 1;
        final InternalStringStats first = withDistinctCharacters(0x20000, half);
        final InternalStringStats second = withDistinctCharacters(0x30000, half);

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> first.reduce(List.of(first, second), emptyReduceContextBuilder().forFinalReduction())
        );

        assertThat(e.getMessage(), containsString("[s]"));
        assertThat(e.getMessage(), containsString("more than [" + InternalStringStats.MAX_DISTINCT_CHARACTERS + "] distinct characters"));
    }

    public void testReduceAtTheLimitOfDistinctCharacters() {
        final InternalStringStats first = withDistinctCharacters(0x20000, InternalStringStats.MAX_DISTINCT_CHARACTERS / 2);
        final InternalStringStats second = withDistinctCharacters(0x30000, InternalStringStats.MAX_DISTINCT_CHARACTERS / 2);

        final InternalStringStats reduced = (InternalStringStats) first.reduce(
            List.of(first, second),
            emptyReduceContextBuilder().forFinalReduction()
        );

        assertEquals(InternalStringStats.MAX_DISTINCT_CHARACTERS, reduced.codePoints().length);
    }

    private static InternalStringStats withDistinctCharacters(int firstCodePoint, int number) {
        final int[] codePoints = new int[number];
        final long[] counts = new long[number];
        for (int i = 0; i < number; i++) {
            codePoints[i] = firstCodePoint + i;
            counts[i] = 1;
        }
        return new InternalStringStats("s", null, 1, number, number, codePoints, counts, false);
    }

    public void testValueByName() {
        final InternalStringStats stats = fromValues("s", false, DRAFT_EXAMPLE);

        assertEquals(3.0, stats.value("count"), 0d);
        assertEquals(10.0, stats.value("min_length"), 0d);
        assertEquals(15.0, stats.value("max_length"), 0d);
        assertEquals(13.0, stats.value("avg_length"), 0d);
        assertEquals(stats.getEntropy(), stats.value("entropy"), 0d);
        assertEquals(stats.getEntropy(), (double) stats.getProperty(List.of("entropy")), 0d);
        assertSame(stats, stats.getProperty(Collections.emptyList()));
        assertEquals(13.0, stats.sortValue("avg_length"), 0d);
    }

    public void testValueByNameUnknown() {
        final InternalStringStats stats = fromValues("s", true, "abc");

        for (String unknown : List.of("entropy2", "Entropy", "min", "avg", "length", "distribution", "distribution.a", "", "value")) {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> stats.value(unknown));
            assertEquals(
                "Unknown value ["
                    + unknown
                    + "] in string_stats aggregation, expected one of [count, min_length, max_length, avg_length, entropy]",
                e.getMessage()
            );
        }
        expectThrows(IllegalArgumentException.class, () -> stats.getProperty(List.of("nope")));
        expectThrows(IllegalArgumentException.class, () -> stats.getProperty(List.of("count", "entropy")));
        expectThrows(IllegalArgumentException.class, () -> stats.sortValue(null));
    }

    public void testSerializationKeepsEverything() throws IOException {
        final InternalStringStats stats = fromValues("s", true, "x\uD83D\uDE00é", "", "abc");

        final InternalStringStats copy = copyInstance(stats);

        assertEquals(stats, copy);
        assertEquals(stats.hashCode(), copy.hashCode());
        assertTrue(copy.showDistribution());
        assertArrayEquals(stats.codePoints(), copy.codePoints());
        assertArrayEquals(stats.characterCounts(), copy.characterCounts());
        assertEquals(stats.getEntropy(), copy.getEntropy(), 0d);
    }

    public void testSerializationAtTheLimitOfDistinctCharacters() throws IOException {
        final InternalStringStats stats = withDistinctCharacters(0x20000, InternalStringStats.MAX_DISTINCT_CHARACTERS);

        assertEquals(stats, copyInstance(stats));
    }

    public void testSerializationKeepsTheNulCharacter() throws IOException {
        // The characters are sent as the gap to the character before, and U+0000 is the only one whose gap is zero, which the reader may
        // only accept for the first character. It is the first character of every result that has it.
        final InternalStringStats stats = fromValues("s", true, "\u0000a\u0000", "\u0000", "a");
        assertEquals(Map.of(0, 3L, (int) 'a', 2L), frequenciesOf(stats));

        final InternalStringStats copy = copyInstance(stats);

        assertEquals(stats, copy);
        assertArrayEquals(new int[] { 0, 'a' }, copy.codePoints());
        assertArrayEquals(new long[] { 3, 2 }, copy.characterCounts());
        assertEquals(Map.of(0, 3L, (int) 'a', 2L), frequenciesOf(copy));
        assertEquals(stats.getEntropy(), copy.getEntropy(), 0d);
        @SuppressWarnings("unchecked")
        final Map<String, Object> distribution = (Map<String, Object>) renderBody(copy).get("distribution");
        assertEquals(Map.of("\u0000", 0.6, "a", 0.4), distribution);

        // and as the only character
        final InternalStringStats onlyNul = fromValues("s", true, "\u0000");
        assertEquals(onlyNul, copyInstance(onlyNul));
        assertArrayEquals(new int[] { 0 }, copyInstance(onlyNul).codePoints());
    }

    public void testNotWrittenToNodesBeforeTheVersionThatAddedIt() {
        final InternalStringStats stats = fromValues("s", false, "abc");

        final BytesStreamOutput out = new BytesStreamOutput();
        out.setVersion(Version.V_3_9_0);
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> out.writeNamedWriteable(stats));

        assertThat(e.getMessage(), containsString("[string_stats]"));
        assertThat(e.getMessage(), containsString("3.11.0"));
    }

    public void testRejectsBytesThatAreNotAResult() throws IOException {
        // the crafted bytes are valid, which shows that the cases below are rejected for what is wrong in them
        final InternalStringStats good = read(out -> writeBody(out, 2, 1, 3, false, new int[] { 'a', 'b' }, new long[] { 3, 2 }));
        assertEquals(2, good.getCount());
        assertEquals(5, good.getAvgLength() * good.getCount(), 1e-9);

        // fewer than no values
        assertCorrupt(out -> {
            writeHeader(out);
            // the largest unsigned 64-bit number, which is -1 as a long, in the 10 bytes of a variable length long
            for (int i = 0; i < 9; i++) {
                out.writeByte((byte) 0xFF);
            }
            out.writeByte((byte) 0x01);
            out.writeBoolean(false);
            out.writeVInt(0);
        });
        // code points that are not in ascending order, or that repeat
        assertCorrupt(out -> writeBody(out, 2, 1, 3, false, new int[] { 'b', 'a' }, new long[] { 1, 1 }));
        assertCorrupt(out -> writeBody(out, 2, 1, 3, false, new int[] { 'a', 'a' }, new long[] { 1, 1 }));
        // a surrogate is half a character, and nothing above U+10FFFF is a character
        assertCorrupt(out -> writeBody(out, 1, 1, 1, false, new int[] { 0xD800 }, new long[] { 1 }));
        assertCorrupt(out -> writeBody(out, 1, 1, 1, false, new int[] { Character.MAX_CODE_POINT + 1 }, new long[] { 1 }));
        // a character that is there zero times
        assertCorrupt(out -> writeBody(out, 1, 1, 1, false, new int[] { 'a' }, new long[] { 0 }));
        // characters without values to have them
        assertCorrupt(out -> writeBody(out, 0, 0, 0, false, new int[] { 'a' }, new long[] { 1 }));
        // a minimum that is above the maximum
        assertCorrupt(out -> writeBody(out, 2, 5, 3, false, new int[] { 'a' }, new long[] { 3 }));
        // more characters than can be stored
        assertCorrupt(out -> {
            writeHeader(out);
            out.writeVLong(1);
            out.writeVInt(1);
            out.writeVInt(1);
            out.writeBoolean(false);
            out.writeVInt(InternalStringStats.MAX_DISTINCT_CHARACTERS + 1);
        });
        // a count of characters that does not fit a number
        assertCorrupt(out -> writeBody(out, 1, 1, 1, false, new int[] { 'a', 'b' }, new long[] { Long.MAX_VALUE, Long.MAX_VALUE }));
    }

    private static void writeHeader(StreamOutput out) throws IOException {
        out.writeString("s");
        out.writeGenericValue(Collections.emptyMap());
    }

    private static void writeBody(
        StreamOutput out,
        long count,
        int minLength,
        int maxLength,
        boolean showDistribution,
        int[] codePoints,
        long[] counts
    ) throws IOException {
        writeHeader(out);
        out.writeVLong(count);
        if (count > 0) {
            out.writeVInt(minLength);
            out.writeVInt(maxLength);
        }
        out.writeBoolean(showDistribution);
        out.writeVInt(codePoints.length);
        int previous = 0;
        for (int i = 0; i < codePoints.length; i++) {
            // the code points are written as the gap to the one before, so a code point that is not above its predecessor is a gap of
            // zero or below, which is the corrupt input that these tests are about
            out.writeVInt(codePoints[i] - previous);
            previous = codePoints[i];
            out.writeVLong(counts[i]);
        }
    }

    private static InternalStringStats read(CheckedConsumer<StreamOutput, IOException> bytes) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            bytes.accept(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new InternalStringStats(in);
            }
        }
    }

    private static void assertCorrupt(CheckedConsumer<StreamOutput, IOException> bytes) {
        expectThrows(IOException.class, () -> read(bytes));
    }

    public void testMutatedInstanceIsNotEqual() throws IOException {
        for (int i = 0; i < 20; i++) {
            final InternalStringStats instance = createTestInstance();
            assertNotEquals(instance, mutateInstance(instance));
        }
    }

    /**
     * Renders the aggregation as JSON, and parses the JSON, which keeps the order of its fields.
     */
    private Map<String, Object> renderBody(InternalStringStats stats) throws IOException {
        final String json = Strings.toString(XContentType.JSON, stats);
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, json)) {
            final Map<String, Object> outer = parser.mapOrdered();
            assertThat(outer.keySet(), equalTo(Set.of(stats.getName())));
            @SuppressWarnings("unchecked")
            final Map<String, Object> body = (Map<String, Object>) outer.get(stats.getName());
            return body;
        }
    }

    public void testHashCodeAndEqualsLookAtTheCharacters() {
        final InternalStringStats one = fromValues("s", false, "ab");
        final InternalStringStats same = fromValues("s", false, "ba");
        final InternalStringStats other = fromValues("s", false, "ac");

        assertEquals(one, same);
        assertEquals(one.hashCode(), same.hashCode());
        assertNotEquals(one, other);
    }
}
