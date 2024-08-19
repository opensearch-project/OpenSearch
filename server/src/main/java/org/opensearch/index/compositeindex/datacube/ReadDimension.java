/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;

import java.io.IOException;
import java.util.Objects;

/**
 * Composite index merge dimension class
 *
 * @opensearch.experimental
 */
public class ReadDimension implements Dimension {
    public static final String READ = "read";
    private final String field;

    public ReadDimension(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
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
