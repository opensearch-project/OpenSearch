/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Composite index numeric dimension class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class NumericDimension implements Dimension {
    public static final String NUMERIC = "numeric";
    private final String field;

    public NumericDimension(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    @Override
    public int getNumSubDimensions() {
        return 1;
    }

    @Override
    public void setDimensionValues(final Long val, final Consumer<Long> dimSetter) {
        dimSetter.accept(val);
    }

    @Override
    public List<String> getSubDimensionNames() {
        // TODO : revisit this post file format changes
        return List.of(field);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CompositeDataCubeFieldType.NAME, field);
        builder.field(CompositeDataCubeFieldType.TYPE, NUMERIC);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NumericDimension dimension = (NumericDimension) o;
        return Objects.equals(field, dimension.getField());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }
}
