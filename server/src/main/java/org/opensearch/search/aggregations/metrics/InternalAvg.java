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
 * Implementation of internal agg
 *
 * @opensearch.internal
 */
public class InternalAvg extends InternalNumericMetricsAggregation.SingleValue implements Avg {
    private final double sum;
    private final long count;

    public InternalAvg(String name, double sum, long count, DocValueFormat format, Map<String, Object> metadata) {
        super(name, metadata);
        this.sum = sum;
        this.count = count;
        this.format = format;
    }

    /**
     * Read from a stream.
     */
    public InternalAvg(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        sum = in.readDouble();
        count = in.readVLong();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(sum);
        out.writeVLong(count);
    }

    @Override
    public double value() {
        return getValue();
    }

    @Override
    public double getValue() {
        return sum / count;
    }

    double getSum() {
        return sum;
    }

    long getCount() {
        return count;
    }

    DocValueFormat getFormatter() {
        return format;
    }

    @Override
    public String getWriteableName() {
        return AvgAggregationBuilder.NAME;
    }

    @Override
    public InternalAvg reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        long count = 0;
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        for (InternalAggregation aggregation : aggregations) {
            if (aggregation instanceof InternalScriptedMetric) {
                // If using InternalScriptedMetric in place of InternalAvg
                Object value = ((InternalScriptedMetric) aggregation).aggregation();
                if (value instanceof ScriptedAvg scriptedAvg) {
                    count += scriptedAvg.getCount();
                    kahanSummation.add(scriptedAvg.getSum());
                } else {
                    throw new IllegalArgumentException(
                        "Invalid ScriptedMetric result for ["
                            + getName()
                            + "] avg aggregation. Expected ScriptedAvg "
                            + "but received ["
                            + (value == null ? "null" : value.getClass().getName())
                            + "]"
                    );
                }
            } else {
                // Original handling for InternalAvg
                InternalAvg avg = (InternalAvg) aggregation;
                count += avg.count;
                kahanSummation.add(avg.sum);
            }
        }
        return new InternalAvg(getName(), kahanSummation.value(), count, format, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), count != 0 ? getValue() : null);
        if (count != 0 && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(getValue()).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sum, count, format.getWriteableName());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalAvg other = (InternalAvg) obj;
        return Objects.equals(sum, other.sum)
            && Objects.equals(count, other.count)
            && Objects.equals(format.getWriteableName(), other.format.getWriteableName());
    }
}
