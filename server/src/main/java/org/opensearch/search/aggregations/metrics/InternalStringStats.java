/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.Version;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the {@code string_stats} aggregation: the number of values, their shortest, longest and average length, and how often each
 * character occurs in them, from which the Shannon entropy and the distribution of the characters follow.
 * <p>
 * The occurrences of every character are kept, and not only the entropy, because the entropy of values that are spread over shards can
 * only be had from the counts of all of them together: it is not any combination of the entropies of the parts. The counts are kept in
 * ascending order of the character, so that the entropy sums the same terms in the same order whichever shards the values came from and
 * in whichever order the shards answer.
 *
 * @opensearch.internal
 */
public class InternalStringStats extends InternalNumericMetricsAggregation.MultiValue implements StringStats {

    /**
     * The name of the aggregation, which is also its type in requests and responses.
     */
    public static final String NAME = "string_stats";

    /**
     * The most distinct characters that the values of one bucket may have. Unicode defines more than a million code points, and the
     * counts of every character are kept for every bucket, sent between nodes, and held while results are merged, so the number of
     * distinct characters has a bound. Real text stays far below it: a corpus of text in many languages has tens of thousands of
     * distinct characters at the most.
     */
    public static final int MAX_DISTINCT_CHARACTERS = 65_536;

    static final String DISTRIBUTION_FIELD = "distribution";

    private static final int[] NO_CODE_POINTS = new int[0];
    private static final long[] NO_COUNTS = new long[0];
    private static final double LN_2 = Math.log(2);

    /**
     * The statistics that other aggregations can use, for example in {@code buckets_path} or to order the buckets of a terms
     * aggregation. The distribution is not one of them, as it is not a number.
     *
     * @opensearch.internal
     */
    enum Metrics {
        count,
        min_length,
        max_length,
        avg_length,
        entropy;

        static Metrics resolve(String name) {
            for (Metrics metric : values()) {
                if (metric.name().equals(name)) {
                    return metric;
                }
            }
            throw new IllegalArgumentException(
                "Unknown value [" + name + "] in " + NAME + " aggregation, expected one of " + Arrays.toString(values())
            );
        }
    }

    /**
     * Calculates the Shannon entropy in bits of characters that occur as often as the counts say.
     * <p>
     * The terms are summed in the order of the counts. The same counts in the same order, which are those in ascending order of the
     * character, always give exactly the same entropy.
     *
     * @return the entropy, which is zero if there are no characters
     */
    static double entropy(long[] counts, long totalCharacters) {
        double entropy = 0.0;
        if (totalCharacters > 0) {
            for (long count : counts) {
                final double probability = (double) count / totalCharacters;
                entropy -= probability * (Math.log(probability) / LN_2);
            }
        }
        return entropy;
    }

    /**
     * Fails if the stream is to a node that does not know this aggregation, so that a mixed-version cluster rejects the request
     * with the reason, instead of that node failing on a name it does not recognise.
     */
    static void ensureSupportedBy(StreamOutput out) {
        if (out.getVersion().before(Version.V_3_11_0)) {
            throw new IllegalArgumentException(
                "["
                    + NAME
                    + "] aggregation requires every node of the cluster to be on version "
                    + Version.V_3_11_0
                    + " or later, but it would be sent to a node on version "
                    + out.getVersion()
            );
        }
    }

    /**
     * @return the result of no values at all
     */
    static InternalStringStats empty(String name, Map<String, Object> metadata, boolean showDistribution) {
        return new InternalStringStats(name, metadata, 0, 0, 0, NO_CODE_POINTS, NO_COUNTS, showDistribution);
    }

    static IllegalArgumentException tooManyDistinctCharacters(String name) {
        return new IllegalArgumentException(
            "["
                + NAME
                + "] aggregation ["
                + name
                + "] found more than ["
                + MAX_DISTINCT_CHARACTERS
                + "] distinct characters in a bucket. Narrow the documents that are aggregated, for example with a filter or a query, "
                + "or aggregate a field with a smaller set of characters"
        );
    }

    private final long count;
    private final int minLength;
    private final int maxLength;
    private final int[] codePoints;
    private final long[] characterCounts;
    private final boolean showDistribution;
    private final long totalCharacters;

    /** The entropy, which is calculated when it is first needed, and is never {@code NaN} once it is. */
    private volatile double entropy = Double.NaN;

    /**
     * Creates the result of values of which the characters occur as often as {@code characterCounts} says.
     *
     * @param count            the number of values
     * @param minLength        the length of the shortest value, which is ignored if there are no values
     * @param maxLength        the length of the longest value, which is ignored if there are no values
     * @param codePoints       the distinct characters in ascending order
     * @param characterCounts  how often each of the characters occurs, which is more than zero
     * @param showDistribution whether the distribution of the characters is part of the response
     */
    InternalStringStats(
        String name,
        Map<String, Object> metadata,
        long count,
        int minLength,
        int maxLength,
        int[] codePoints,
        long[] characterCounts,
        boolean showDistribution
    ) {
        super(name, metadata);
        assert codePoints.length == characterCounts.length;
        assert count > 0 || codePoints.length == 0 : "characters without values";
        this.count = count;
        this.minLength = count > 0 ? minLength : 0;
        this.maxLength = count > 0 ? maxLength : 0;
        this.codePoints = codePoints;
        this.characterCounts = characterCounts;
        this.showDistribution = showDistribution;
        long total = 0;
        for (int i = 0; i < codePoints.length; i++) {
            assert i == 0 || codePoints[i - 1] < codePoints[i] : "the characters are not in ascending order";
            assert characterCounts[i] > 0 : "a character that does not occur";
            total += characterCounts[i];
        }
        this.totalCharacters = total;
    }

    /**
     * Read from a stream.
     */
    public InternalStringStats(StreamInput in) throws IOException {
        super(in);
        final long count = in.readVLong();
        if (count < 0) {
            throw corrupt("it has [" + count + "] values");
        }
        int minLength = 0;
        int maxLength = 0;
        if (count > 0) {
            minLength = in.readVInt();
            maxLength = in.readVInt();
            if (minLength < 0 || minLength > maxLength) {
                throw corrupt(
                    "the shortest value of [" + minLength + "] characters is not between zero and the longest of [" + maxLength + "]"
                );
            }
        }
        final boolean showDistribution = in.readBoolean();
        final int distinct = in.readVInt();
        if (distinct < 0 || distinct > MAX_DISTINCT_CHARACTERS) {
            throw corrupt("[" + distinct + "] distinct characters is not between zero and " + MAX_DISTINCT_CHARACTERS);
        }
        if (count == 0 && distinct > 0) {
            throw corrupt("it has characters but no values");
        }
        final int[] codePoints = distinct == 0 ? NO_CODE_POINTS : new int[distinct];
        final long[] characterCounts = distinct == 0 ? NO_COUNTS : new long[distinct];
        long total = 0;
        int previous = 0;
        for (int i = 0; i < distinct; i++) {
            // the characters are sent as the gap to the character before, which keeps the bytes few, as most are close to each other
            final int gap = in.readVInt();
            if (gap < 0 || (i > 0 && gap == 0)) {
                throw corrupt("the characters are not in ascending order");
            }
            final long codePoint = (long) previous + gap;
            if (codePoint > Character.MAX_CODE_POINT || (codePoint >= Character.MIN_SURROGATE && codePoint <= Character.MAX_SURROGATE)) {
                throw corrupt("[" + codePoint + "] is not a character");
            }
            final long occurrences = in.readVLong();
            if (occurrences <= 0 || total > Long.MAX_VALUE - occurrences) {
                throw corrupt("a character occurs [" + occurrences + "] times");
            }
            codePoints[i] = (int) codePoint;
            characterCounts[i] = occurrences;
            previous = (int) codePoint;
            total += occurrences;
        }
        this.count = count;
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.showDistribution = showDistribution;
        this.codePoints = codePoints;
        this.characterCounts = characterCounts;
        this.totalCharacters = total;
    }

    private IOException corrupt(String reason) {
        return new IOException("Corrupt [" + NAME + "] aggregation [" + name + "]: " + reason);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        ensureSupportedBy(out);
        out.writeVLong(count);
        if (count > 0) {
            out.writeVInt(minLength);
            out.writeVInt(maxLength);
        }
        out.writeBoolean(showDistribution);
        out.writeVInt(codePoints.length);
        int previous = 0;
        for (int i = 0; i < codePoints.length; i++) {
            out.writeVInt(codePoints[i] - previous);
            out.writeVLong(characterCounts[i]);
            previous = codePoints[i];
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long count = 0;
        int minLength = Integer.MAX_VALUE;
        int maxLength = 0;
        final CharacterCounts merged = new CharacterCounts();
        for (InternalAggregation aggregation : aggregations) {
            final InternalStringStats stats = (InternalStringStats) aggregation;
            if (stats.count == 0) {
                // without values there is no shortest value, and zero must not be taken for the length of one
                continue;
            }
            count += stats.count;
            minLength = Math.min(minLength, stats.minLength);
            maxLength = Math.max(maxLength, stats.maxLength);
            for (int i = 0; i < stats.codePoints.length; i++) {
                merged.add(stats.codePoints[i], stats.characterCounts[i]);
            }
            if (merged.size() > MAX_DISTINCT_CHARACTERS) {
                throw tooManyDistinctCharacters(name);
            }
        }
        if (count == 0) {
            return empty(name, metadata, showDistribution);
        }
        final int[] codePoints = merged.sortedCodePoints();
        return new InternalStringStats(
            name,
            metadata,
            count,
            minLength,
            maxLength,
            codePoints,
            merged.countsOf(codePoints),
            showDistribution
        );
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Metrics.count.name(), count);
        if (count > 0) {
            builder.field(Metrics.min_length.name(), minLength);
            builder.field(Metrics.max_length.name(), maxLength);
            builder.field(Metrics.avg_length.name(), getAvgLength());
            builder.field(Metrics.entropy.name(), getEntropy());
            if (showDistribution) {
                builder.startObject(DISTRIBUTION_FIELD);
                for (Map.Entry<String, Double> entry : getDistribution().entrySet()) {
                    builder.field(entry.getKey(), entry.getValue().doubleValue());
                }
                builder.endObject();
            }
        } else {
            builder.nullField(Metrics.min_length.name());
            builder.nullField(Metrics.max_length.name());
            builder.nullField(Metrics.avg_length.name());
            builder.nullField(Metrics.entropy.name());
            if (showDistribution) {
                builder.nullField(DISTRIBUTION_FIELD);
            }
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            super.hashCode(),
            count,
            minLength,
            maxLength,
            showDistribution,
            Arrays.hashCode(codePoints),
            Arrays.hashCode(characterCounts)
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalStringStats other = (InternalStringStats) obj;
        return count == other.count
            && minLength == other.minLength
            && maxLength == other.maxLength
            && showDistribution == other.showDistribution
            && Arrays.equals(codePoints, other.codePoints)
            && Arrays.equals(characterCounts, other.characterCounts);
    }

    /**
     * @return the distinct characters of the values in ascending order, which the caller must not change
     */
    int[] codePoints() {
        return codePoints;
    }

    /**
     * @return how often each of the {@link #codePoints()} occurs, which the caller must not change
     */
    long[] characterCounts() {
        return characterCounts;
    }

    /**
     * @return the length of the shortest value, or zero if there are no values
     */
    int minLength() {
        return minLength;
    }

    /**
     * @return the length of the longest value, or zero if there are no values
     */
    int maxLength() {
        return maxLength;
    }

    /**
     * @return whether the distribution of the characters is part of the response
     */
    boolean showDistribution() {
        return showDistribution;
    }

    @Override
    public double value(String name) {
        switch (Metrics.resolve(name)) {
            case count:
                return getCount();
            case min_length:
                return getMinLength();
            case max_length:
                return getMaxLength();
            case avg_length:
                return getAvgLength();
            case entropy:
                return getEntropy();
            default:
                throw new AssertionError("unhandled metric [" + name + "]");
        }
    }

    @Override
    public long getCount() {
        return count;
    }

    @Override
    public double getMinLength() {
        return count > 0 ? minLength : Double.NaN;
    }

    @Override
    public double getMaxLength() {
        return count > 0 ? maxLength : Double.NaN;
    }

    @Override
    public double getAvgLength() {
        return count > 0 ? (double) totalCharacters / count : Double.NaN;
    }

    @Override
    public double getEntropy() {
        if (count == 0) {
            return Double.NaN;
        }
        double result = entropy;
        if (Double.isNaN(result)) {
            result = entropy(characterCounts, totalCharacters);
            entropy = result;
        }
        return result;
    }

    @Override
    public Map<String, Double> getDistribution() {
        if (showDistribution == false || totalCharacters == 0) {
            return Collections.emptyMap();
        }
        final Integer[] order = new Integer[codePoints.length];
        for (int i = 0; i < order.length; i++) {
            order[i] = i;
        }
        // the most frequent characters first, and the characters that are as frequent as each other in ascending order
        Arrays.sort(order, (a, b) -> {
            final int byCount = Long.compare(characterCounts[b], characterCounts[a]);
            return byCount != 0 ? byCount : Integer.compare(codePoints[a], codePoints[b]);
        });
        final Map<String, Double> distribution = new LinkedHashMap<>();
        for (int index : order) {
            distribution.put(new String(codePoints, index, 1), (double) characterCounts[index] / totalCharacters);
        }
        return Collections.unmodifiableMap(distribution);
    }
}
