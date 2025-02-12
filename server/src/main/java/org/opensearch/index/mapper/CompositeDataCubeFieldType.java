/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base class for multi field data cube fields
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class CompositeDataCubeFieldType extends CompositeMappedFieldType {
    public static final String NAME = "name";
    public static final String TYPE = "type";
    private final List<Dimension> dimensions;
    private final List<Metric> metrics;

    public CompositeDataCubeFieldType(String name, List<Dimension> dims, List<Metric> metrics, CompositeFieldType type) {
        super(name, getFields(dims, metrics), type);
        this.dimensions = dims;
        this.metrics = metrics;
    }

    private static List<String> getFields(List<Dimension> dims, List<Metric> metrics) {
        Set<String> fields = new HashSet<>();
        for (Dimension dim : dims) {
            fields.add(dim.getField());
        }
        for (Metric metric : metrics) {
            fields.add(metric.getField());
        }
        return new ArrayList<>(fields);
    }

    public List<Dimension> getDimensions() {
        return dimensions;
    }

    public List<Metric> getMetrics() {
        return metrics;
    }
}
