/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Entity for Weighted Round Robin weights
 *
 * @opensearch.internal
 */
public class WRRWeights implements Writeable {
    private String attributeName;
    private Map<String, Object> weights;

    public WRRWeights(String attributeName, Map<String, Object> weights) {
        this.attributeName = attributeName;
        this.weights = weights;
    }

    public WRRWeights(WRRWeights wrrWeight) {
        this.attributeName = wrrWeight.attributeName();
        this.weights = wrrWeight.weights;
    }

    public WRRWeights(StreamInput in) throws IOException {
        attributeName = in.readString();
        weights = in.readMap();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(attributeName);
        out.writeMap(weights);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WRRWeights that = (WRRWeights) o;

        if (!attributeName.equals(that.attributeName)) return false;
        return weights.equals(that.weights);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeName, weights);
    }

    @Override
    public String toString() {
        return "WRRWeightsDefinition{" + attributeName + "}{" + weights().toString() + "}";
    }

    public Map<String, Object> weights() {
        return this.weights;
    }

    public String attributeName() {
        return this.attributeName;
    }
}
