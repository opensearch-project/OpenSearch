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
 * Implementation of median absolute deviation agg
 *
 * @opensearch.internal
 */
public class InternalMedianAbsoluteDeviation extends InternalNumericMetricsAggregation.SingleValue implements MedianAbsoluteDeviation {

    static double computeMedianAbsoluteDeviation(TDigestState valuesSketch) {

        if (valuesSketch.size() == 0) {
            return Double.NaN;
        } else {
            final double approximateMedian = valuesSketch.quantile(0.5);
            final TDigestState approximatedDeviationsSketch = new TDigestState(valuesSketch.compression());
            valuesSketch.centroids().forEach(centroid -> {
                final double deviation = Math.abs(approximateMedian - centroid.mean());
                approximatedDeviationsSketch.add(deviation, centroid.count());
            });

            return approximatedDeviationsSketch.quantile(0.5);
        }
    }

    private final TDigestState valuesSketch;
    private final double medianAbsoluteDeviation;

    InternalMedianAbsoluteDeviation(String name, Map<String, Object> metadata, DocValueFormat format, TDigestState valuesSketch) {
        super(name, metadata);
        this.format = Objects.requireNonNull(format);
        this.valuesSketch = Objects.requireNonNull(valuesSketch);

        this.medianAbsoluteDeviation = computeMedianAbsoluteDeviation(this.valuesSketch);
    }

    public InternalMedianAbsoluteDeviation(StreamInput in) throws IOException {
        super(in);
        format = in.readNamedWriteable(DocValueFormat.class);
        valuesSketch = TDigestState.read(in);
        medianAbsoluteDeviation = in.readDouble();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(format);
        TDigestState.write(valuesSketch, out);
        out.writeDouble(medianAbsoluteDeviation);
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        final TDigestState valueMerged = new TDigestState(valuesSketch.compression());
        for (InternalAggregation aggregation : aggregations) {
            final InternalMedianAbsoluteDeviation madAggregation = (InternalMedianAbsoluteDeviation) aggregation;
            valueMerged.add(madAggregation.valuesSketch);
        }

        return new InternalMedianAbsoluteDeviation(name, metadata, format, valueMerged);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        final boolean anyResults = valuesSketch.size() > 0;
        final Double mad = anyResults ? getMedianAbsoluteDeviation() : null;

        builder.field(CommonFields.VALUE.getPreferredName(), mad);
        if (format != DocValueFormat.RAW && anyResults) {
            builder.field(CommonFields.VALUE_AS_STRING.getPreferredName(), format.format(mad).toString());
        }

        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), valuesSketch);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalMedianAbsoluteDeviation other = (InternalMedianAbsoluteDeviation) obj;
        return Objects.equals(valuesSketch, other.valuesSketch);
    }

    @Override
    public String getWriteableName() {
        return MedianAbsoluteDeviationAggregationBuilder.NAME;
    }

    TDigestState getValuesSketch() {
        return valuesSketch;
    }

    @Override
    public double value() {
        return getMedianAbsoluteDeviation();
    }

    @Override
    public double getMedianAbsoluteDeviation() {
        return medianAbsoluteDeviation;
    }
}
