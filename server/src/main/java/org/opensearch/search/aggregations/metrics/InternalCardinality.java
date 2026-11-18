/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of cardinality agg
 *
 * @opensearch.internal
 */
public final class InternalCardinality extends InternalNumericMetricsAggregation.SingleValue implements Cardinality {
    private final AbstractHyperLogLogPlusPlus counts;

    /**
     * Constructs a result from a HyperLogLog++ sketch, for producers that build
     * cardinality results outside the shard-level aggregation path.
     *
     * <p>The reduce phase merges the sketches of all shards, so a sketch built
     * here must be interchangeable with one the shard-level aggregator builds
     * for the same request:
     * <ul>
     *   <li>Values must be collected into bucket ordinal {@code 0}, the only bucket
     *   this result reads.</li>
     *   <li>Its precision must be the one the aggregator uses: the field's mapped
     *   precision for an {@code hll} field, and otherwise
     *   {@link HyperLogLogPlusPlus#DEFAULT_PRECISION} when
     *   {@link CardinalityAggregationBuilder#precisionThreshold()} is {@code null} and
     *   {@link HyperLogLogPlusPlus#precisionFromThreshold(long)} of it when it is set.
     *   Sketches of different precisions cannot be merged.</li>
     *   <li>Each distinct value must be collected under the hash the aggregator gives
     *   its doc value: {@code BitMixer.mix64(value)} for an integer-valued numeric field;
     *   {@code BitMixer.mix64(Double.doubleToLongBits(value))} of the value as a double for
     *   a floating-point or {@code unsigned_long} field; and for any other field, the
     *   {@code h1} half of {@code MurmurHash3.hash128} with seed 0 over the doc-value bytes
     *   (the encoded form, such as the 16-byte encoding of an {@code ip}, not its string
     *   form). A different hash still merges, but values shared with other shards are then
     *   counted more than once.</li>
     * </ul>
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public InternalCardinality(String name, AbstractHyperLogLogPlusPlus counts, Map<String, Object> metadata) {
        super(name, metadata);
        this.counts = counts;
    }

    /**
     * Read from a stream.
     */
    public InternalCardinality(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        if (in.readBoolean()) {
            counts = AbstractHyperLogLogPlusPlus.readFrom(in, BigArrays.NON_RECYCLING_INSTANCE);
        } else {
            counts = null;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        if (counts != null) {
            out.writeBoolean(true);
            counts.writeTo(0, out);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public String getWriteableName() {
        return CardinalityAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public long getValue() {
        return counts == null ? 0 : counts.cardinality(0);
    }

    public AbstractHyperLogLogPlusPlus getCounts() {
        return counts;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        HyperLogLogPlusPlus reduced = null;
        for (InternalAggregation aggregation : aggregations) {
            final InternalCardinality cardinality = (InternalCardinality) aggregation;
            if (cardinality.counts != null) {
                if (reduced == null) {
                    reduced = new HyperLogLogPlusPlus(cardinality.counts.precision(), BigArrays.NON_RECYCLING_INSTANCE, 1);
                }
                reduced.merge(0, cardinality.counts, 0);
            }
        }

        if (reduced == null) { // all empty
            return aggregations.get(0);
        } else {
            return new InternalCardinality(name, reduced, getMetadata());
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final long cardinality = getValue();
        builder.field(CommonFields.VALUE.getPreferredName(), cardinality);
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), counts.hashCode(0));
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalCardinality other = (InternalCardinality) obj;
        return counts.equals(0, other.counts, 0);
    }

    AbstractHyperLogLogPlusPlus getState() {
        return counts;
    }
}
