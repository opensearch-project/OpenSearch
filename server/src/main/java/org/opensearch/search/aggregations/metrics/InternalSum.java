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

import org.opensearch.Version;
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
 * Implementation of sum agg
 *
 * @opensearch.internal
 */
public class InternalSum extends InternalNumericMetricsAggregation.SingleValue implements Sum {
    private final double sum;
    private final long count;

    public InternalSum(String name, double sum, long count, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, metadata);
        this.sum = sum;
        this.count = count;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    public InternalSum(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        sum = in.readDouble();
        if (in.getVersion().onOrAfter(Version.V_3_6_0)) {
            count = in.readVLong();
        } else {
            // Legacy nodes do not send count; default to 1 so that the sum
            // is always rendered as a numeric value during mixed-version reduce,
            // preserving the old behaviour until all nodes are upgraded.
            count = 1;
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(sum);
        if (out.getVersion().onOrAfter(Version.V_3_6_0)) {
            out.writeVLong(count);
        }
    }

    @Override
    public String getWriteableName() {
        return SumAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return sum;
    }

    @Override
    public double getValue() {
        return sum;
    }

    public long getCount() {
        return count;
    }

    @Override
    public InternalSum reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // Compute the sum of double values with Kahan summation algorithm which is more
        // accurate than naive summation.
        CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        long count = 0;
        for (InternalAggregation aggregation : aggregations) {
            InternalSum sum = (InternalSum) aggregation;
            count += sum.count;
            kahanSummation.add(sum.sum);
        }
        return new InternalSum(name, kahanSummation.value(), count, format, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (count != 0) {
            builder.field(CommonFields.VALUE.getPreferredName(), sum);
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(sum).toString());
            }
        } else {
            builder.nullField(CommonFields.VALUE.getPreferredName());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), sum, count);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalSum that = (InternalSum) obj;
        return Objects.equals(sum, that.sum) && count == that.count;
    }
}
