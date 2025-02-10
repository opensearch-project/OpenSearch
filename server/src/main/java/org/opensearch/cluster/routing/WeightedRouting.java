/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Entity for Weighted Round Robin weights
 *
 * @opensearch.api
 */
@PublicApi(since = "2.4.0")
public class WeightedRouting implements Writeable {
    private final String attributeName;
    private final Map<String, Double> weights;
    private final int hashCode;

    public WeightedRouting() {
        this("", new HashMap<>(3));
    }

    public WeightedRouting(String attributeName, Map<String, Double> weights) {
        this.attributeName = attributeName;
        this.weights = Collections.unmodifiableMap(weights);
        this.hashCode = Objects.hash(this.attributeName, this.weights);
    }

    public WeightedRouting(WeightedRouting weightedRouting) {
        this(weightedRouting.attributeName(), weightedRouting.weights);
    }

    public WeightedRouting(StreamInput in) throws IOException {
        this(in.readString(), (Map<String, Double>) in.readGenericValue());
    }

    public boolean isSet() {
        return this.attributeName != null && !this.attributeName.isEmpty() && this.weights != null && !this.weights.isEmpty();
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
        return hashCode;
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
