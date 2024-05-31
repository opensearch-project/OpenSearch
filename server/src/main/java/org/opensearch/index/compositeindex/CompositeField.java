/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;

/**
 * Composite field which contains dimensions, metrics and index mode specific specs
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeField {
    private final String name;
    private final List<Dimension> dimensionsOrder;
    private final List<Metric> metrics;
    private final CompositeFieldSpec compositeFieldSpec;

    public CompositeField(String name, List<Dimension> dimensions, List<Metric> metrics, CompositeFieldSpec compositeFieldSpec) {
        this.name = name;
        this.dimensionsOrder = dimensions;
        this.metrics = metrics;
        this.compositeFieldSpec = compositeFieldSpec;
    }

    public String getName() {
        return name;
    }

    public List<Dimension> getDimensionsOrder() {
        return dimensionsOrder;
    }

    public List<Metric> getMetrics() {
        return metrics;
    }

    public CompositeFieldSpec getSpec() {
        return compositeFieldSpec;
    }
}
