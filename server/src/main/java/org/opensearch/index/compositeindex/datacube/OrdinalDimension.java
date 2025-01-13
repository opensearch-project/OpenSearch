/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Composite index keyword dimension class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class OrdinalDimension implements Dimension {
    public static final String ORDINAL = "ordinal";
    private final String field;

    public OrdinalDimension(String field) {
        this.field = field;
    }

    @Override
    public String getField() {
        return field;
    }

    @Override
    public int getNumSubDimensions() {
        return 1;
    }

    @Override
    public void setDimensionValues(Long value, Consumer<Long> dimSetter) {
        // This will set the keyword dimension value's ordinal
        dimSetter.accept(value);
    }

    @Override
    public List<String> getSubDimensionNames() {
        return List.of(field);
    }

    @Override
    public DocValuesType getDocValuesType() {
        return DocValuesType.SORTED_SET;
    }

    @Override
    public long convertToOrdinal(Object rawValue, StarTreeValues starTreeValues) {
        StarTreeValuesIterator genericIterator = starTreeValues.getDimensionValuesIterator(field);
        if (genericIterator instanceof SortedSetStarTreeValuesIterator) {
            try {
                return ((SortedSetStarTreeValuesIterator) genericIterator).lookupTerm((BytesRef) rawValue);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new IllegalArgumentException("Unsupported star tree values iterator " + genericIterator.getClass().getName());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CompositeDataCubeFieldType.NAME, field);
        builder.field(CompositeDataCubeFieldType.TYPE, ORDINAL);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrdinalDimension dimension = (OrdinalDimension) o;
        return Objects.equals(field, dimension.getField());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }
}
