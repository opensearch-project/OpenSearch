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
 * Implementation of min agg
 *
 * @opensearch.internal
 */
public class InternalMin extends InternalNumericMetricsAggregation.SingleValue implements Min {
    private final double min;

    public InternalMin(String name, double min, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, metadata);
        this.min = min;
        this.format = formatter;
    }

    /**
     * Read from a stream.
     */
    public InternalMin(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        min = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        out.writeDouble(min);
    }

    @Override
    public String getWriteableName() {
        return MinAggregationBuilder.NAME;
    }

    @Override
    public double value() {
        return min;
    }

    @Override
    public double getValue() {
        return min;
    }

    @Override
    public InternalMin reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        double min = Double.POSITIVE_INFINITY;
        for (InternalAggregation aggregation : aggregations) {
            min = Math.min(min, ((InternalMin) aggregation).min);
        }
        return new InternalMin(getName(), min, this.format, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        boolean hasValue = !Double.isInfinite(min);
        builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? min : null);
        if (hasValue && format != DocValueFormat.RAW) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(min).toString());
        }
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), min);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalMin other = (InternalMin) obj;
        return Objects.equals(min, other.min);
    }

}
