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

    InternalCardinality(String name, AbstractHyperLogLogPlusPlus counts, Map<String, Object> metadata) {
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
