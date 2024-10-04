/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.apache.lucene.index.DocValuesType;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Represents a dimension for reconstructing StarTreeField from file formats during searches and merges.
 *
 * @opensearch.experimental
 */
public class ReadDimension implements Dimension {
    public static final String READ = "read";
    private final String field;
    private final DocValuesType docValuesType;

    public ReadDimension(String field) {
        this.field = field;
        this.docValuesType = DocValuesType.SORTED_NUMERIC;
    }

    public ReadDimension(String field, DocValuesType docValuesType) {
        this.field = field;
        this.docValuesType = docValuesType;
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
        return List.of(field);
    }

    @Override
    public DocValuesType getDocValuesType() {
        return docValuesType;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CompositeDataCubeFieldType.NAME, field);
        builder.field(CompositeDataCubeFieldType.TYPE, READ);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReadDimension dimension = (ReadDimension) o;
        return Objects.equals(field, dimension.getField());
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }

}
