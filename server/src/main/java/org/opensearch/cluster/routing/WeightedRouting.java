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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Entity for Weighted Round Robin weights
 *
 * @opensearch.internal
 */
public class WeightedRouting implements Writeable {
    private String attributeName;
    private Map<String, Double> weights;

    public WeightedRouting() {
        this.attributeName = "";
        this.weights = new HashMap<>(3);
    }

    public WeightedRouting(String attributeName, Map<String, Double> weights) {
        this.attributeName = attributeName;
        this.weights = weights;
    }

    public WeightedRouting(WeightedRouting weightedRouting) {
        this.attributeName = weightedRouting.attributeName();
        this.weights = weightedRouting.weights;
    }

    public WeightedRouting(StreamInput in) throws IOException {
        attributeName = in.readString();
        weights = (Map<String, Double>) in.readGenericValue();
    }

    public boolean isSet() {
        return (!this.attributeName.isEmpty() && !this.weights.isEmpty());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(attributeName);
        out.writeGenericValue(weights);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WeightedRouting that = (WeightedRouting) o;
        if (!attributeName.equals(that.attributeName)) return false;
        return weights.equals(that.weights);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attributeName, weights);
    }

    @Override
    public String toString() {
        return "WeightedRouting{" + attributeName + "}{" + weights().toString() + "}";
    }

    public Map<String, Double> weights() {
        return this.weights;
    }

    public String attributeName() {
        return this.attributeName;
    }
}
