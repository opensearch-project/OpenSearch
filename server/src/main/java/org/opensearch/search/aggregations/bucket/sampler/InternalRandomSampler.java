/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.sampler;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.SamplingContext;
import org.opensearch.search.aggregations.bucket.InternalSingleBucketAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the {@code random_sampler} aggregation.
 * <p>
 * Shards report the number of documents they actually collected. The scaling to an estimate for the whole match set
 * happens once, during the final reduce, so that the per-shard counts are summed before being divided by the sampling
 * probability rather than each being rounded on its own.
 *
 * @opensearch.internal
 */
public class InternalRandomSampler extends InternalSingleBucketAggregation implements RandomSampler {

    public static final String NAME = "random_sampler";
    public static final String PARSER_NAME = "random_sampler";

    private final int seed;
    private final double probability;
    private final long sampledDocCount;

    InternalRandomSampler(
        String name,
        long sampledDocCount,
        int seed,
        double probability,
        InternalAggregations subAggregations,
        Map<String, Object> metadata
    ) {
        this(name, sampledDocCount, sampledDocCount, seed, probability, subAggregations, metadata);
    }

    private InternalRandomSampler(
        String name,
        long docCount,
        long sampledDocCount,
        int seed,
        double probability,
        InternalAggregations subAggregations,
        Map<String, Object> metadata
    ) {
        super(name, docCount, subAggregations, metadata);
        this.sampledDocCount = sampledDocCount;
        this.seed = seed;
        this.probability = probability;
    }

    /**
     * Read from a stream.
     */
    public InternalRandomSampler(StreamInput in) throws IOException {
        super(in);
        this.seed = in.readInt();
        this.probability = in.readDouble();
        this.sampledDocCount = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
        out.writeInt(seed);
        out.writeDouble(probability);
        out.writeVLong(sampledDocCount);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getType() {
        return PARSER_NAME;
    }

    /**
     * The probability the documents in this bucket were sampled with. Carried on the wire, but deliberately not
     * rendered: the response shape stays that of an ordinary single-bucket aggregation.
     */
    public double getProbability() {
        return probability;
    }

    /**
     * The number of documents actually collected, before scaling. Also carried on the wire without being rendered, so
     * that a later release can report it, or a confidence interval derived from it, without a wire-format change.
     */
    public long getSampledDocCount() {
        return sampledDocCount;
    }

    public int getSeed() {
        return seed;
    }

    @Override
    protected InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations) {
        return new InternalRandomSampler(name, docCount, sampledDocCount, seed, probability, subAggregations, metadata);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long sampled = 0L;
        List<InternalAggregations> subAggregationsList = new ArrayList<>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            assert aggregation.getName().equals(getName());
            InternalRandomSampler sampler = (InternalRandomSampler) aggregation;
            sampled += sampler.sampledDocCount;
            subAggregationsList.add(sampler.getAggregations());
        }
        InternalAggregations reducedSubAggregations = InternalAggregations.reduce(subAggregationsList, reduceContext);

        if (reduceContext.isFinalReduce() == false) {
            // A partial reduce may be followed by more of them, so counts stay raw until every shard has been folded in.
            return new InternalRandomSampler(getName(), sampled, sampled, seed, probability, reducedSubAggregations, metadata);
        }

        SamplingContext samplingContext = new SamplingContext(probability);
        return new InternalRandomSampler(
            getName(),
            samplingContext.scaleUp(sampled),
            sampled,
            seed,
            probability,
            reducedSubAggregations.finalizeSampling(samplingContext),
            metadata
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (super.equals(obj) == false) {
            return false;
        }
        InternalRandomSampler other = (InternalRandomSampler) obj;
        return seed == other.seed && Double.compare(probability, other.probability) == 0 && sampledDocCount == other.sampledDocCount;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), seed, probability, sampledDocCount);
    }
}
