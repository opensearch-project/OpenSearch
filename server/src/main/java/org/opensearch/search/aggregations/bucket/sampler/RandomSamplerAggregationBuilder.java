/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.AbstractAggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregatorFactories.Builder;
import org.opensearch.search.aggregations.AggregatorFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Builds the {@code random_sampler} aggregation: a single bucket holding a uniform random sample of the documents
 * matching the query, which lets expensive sub-aggregations run over a fraction of the corpus.
 * <p>
 * <b>Results are estimates.</b> Every {@code doc_count} underneath this aggregation, including this bucket's own, is
 * scaled by {@code 1 / probability}, so it is an estimate of a population count rather than a count. It is rendered as
 * a plain integer and carries no indication of its variance. Treat the numbers, and any statistic derived from them, as
 * approximate. The error grows as {@code probability} falls, and how fast depends on how much the aggregated quantity
 * varies across documents.
 * <p>
 * <b>Use it with broad queries over large indices.</b> Sampling visits about {@code probability * maxDoc} documents per
 * segment <i>whatever the query matches</i>, because the sample is drawn over the whole segment and then intersected
 * with the query. Two consequences follow. With a selective filter the work is unchanged while the number of sampled
 * matches becomes tiny, which makes the estimate very noisy; and for a query that matches few documents the sampling
 * walk can cost more than aggregating every match would have.
 * <p>
 * <b>Not every sub-aggregation is scaled.</b> Bucket {@code doc_count}s, {@code sum}, {@code value_count},
 * {@code stats} and {@code extended_stats} are. Values that already estimate the population are deliberately left
 * alone: averages, minima, maxima, percentiles and variances. {@code cardinality} is not scaled either, because a
 * count of distinct values in a sample cannot be extrapolated by multiplication; compare it against this bucket's
 * {@code doc_count} instead. {@code sampler} and {@code diversified_sampler} are not scaled, since their
 * {@code doc_count} is the number of top-scoring documents they kept rather than a count of matches, and neither is
 * anything nested under them. Any other aggregation reports the value measured on the sample.
 * <p>
 * A {@code nested} sub-aggregation is scaled, but its counts are noisier than a direct document sample at the same
 * probability, because sampling picks parents and then takes all of their children.
 * <p>
 * This is not {@link SamplerAggregationBuilder}: {@code sampler} keeps the top-scoring {@code shard_size} documents of
 * each shard, which is a truncation by relevance, while this one keeps a uniform random fraction and never looks at
 * scores.
 *
 * @opensearch.internal
 */
public class RandomSamplerAggregationBuilder extends AbstractAggregationBuilder<RandomSamplerAggregationBuilder> {

    public static final String NAME = "random_sampler";

    public static final ParseField PROBABILITY_FIELD = new ParseField("probability");
    public static final ParseField SEED_FIELD = new ParseField("seed");

    /**
     * Sampling at half the documents or more costs about as much as not sampling while still perturbing the result, so
     * probabilities in {@code [0.5, 1.0)} are rejected rather than silently accepted.
     */
    public static final double MAX_SAMPLING_PROBABILITY = 0.5;

    public static final ConstructingObjectParser<RandomSamplerAggregationBuilder, String> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        (args, name) -> new RandomSamplerAggregationBuilder(name).probability((double) args[0])
    );
    static {
        // A constructor argument rather than an optional field, so that omitting it is rejected with a clear message
        // instead of silently sampling nothing.
        PARSER.declareDouble(constructorArg(), PROBABILITY_FIELD);
        PARSER.declareInt(RandomSamplerAggregationBuilder::seed, SEED_FIELD);
    }

    private double probability = Double.NaN;
    private Integer seed;

    public RandomSamplerAggregationBuilder(String name) {
        super(name);
    }

    protected RandomSamplerAggregationBuilder(
        RandomSamplerAggregationBuilder clone,
        Builder factoriesBuilder,
        Map<String, Object> metadata
    ) {
        super(clone, factoriesBuilder, metadata);
        this.probability = clone.probability;
        this.seed = clone.seed;
    }

    /**
     * Read from a stream.
     */
    public RandomSamplerAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.probability = in.readDouble();
        this.seed = in.readOptionalInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(probability);
        out.writeOptionalInt(seed);
    }

    @Override
    protected AggregationBuilder shallowCopy(Builder factoriesBuilder, Map<String, Object> metadata) {
        return new RandomSamplerAggregationBuilder(this, factoriesBuilder, metadata);
    }

    /**
     * The probability that any given document is sampled, in {@code (0, 0.5)}, or exactly {@code 1.0} to sample
     * nothing away.
     */
    public RandomSamplerAggregationBuilder probability(double probability) {
        if (probability == 1.0) {
            this.probability = probability;
            return this;
        }
        if (Double.isNaN(probability) || probability <= 0.0 || probability >= MAX_SAMPLING_PROBABILITY) {
            throw new IllegalArgumentException(
                "["
                    + PROBABILITY_FIELD.getPreferredName()
                    + "] must be greater than 0.0 and less than "
                    + MAX_SAMPLING_PROBABILITY
                    + ", or exactly 1.0, but was ["
                    + probability
                    + "]"
            );
        }
        this.probability = probability;
        return this;
    }

    public double probability() {
        return probability;
    }

    /**
     * The seed for the sampling. With a seed, the sample repeats between calls for unchanged data; without one, a seed
     * is drawn per shard per request, so each call sees a different sample.
     */
    public RandomSamplerAggregationBuilder seed(int seed) {
        this.seed = seed;
        return this;
    }

    public Integer seed() {
        return seed;
    }

    @Override
    public BucketCardinality bucketCardinality() {
        return BucketCardinality.ONE;
    }

    @Override
    protected AggregatorFactory doBuild(QueryShardContext queryShardContext, AggregatorFactory parent, Builder subFactoriesBuilder)
        throws IOException {
        if (Double.isNaN(probability)) {
            throw new IllegalArgumentException("[" + PROBABILITY_FIELD.getPreferredName() + "] must be set on [" + name + "]");
        }
        return new RandomSamplerAggregatorFactory(name, probability, seed, queryShardContext, parent, subFactoriesBuilder, metadata);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PROBABILITY_FIELD.getPreferredName(), probability);
        if (seed != null) {
            builder.field(SEED_FIELD.getPreferredName(), seed);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), probability, seed);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (super.equals(obj) == false) {
            return false;
        }
        RandomSamplerAggregationBuilder other = (RandomSamplerAggregationBuilder) obj;
        return Double.compare(probability, other.probability) == 0 && Objects.equals(seed, other.seed);
    }

    @Override
    public String getType() {
        return NAME;
    }
}
