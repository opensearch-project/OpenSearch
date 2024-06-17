/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.StarTreeMapper;

import java.io.IOException;
import java.util.Objects;

/**
 * Composite index dimension base class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Dimension implements ToXContent {
    public static final String NUMERIC = "numeric";
    private final String field;

    public Dimension(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(StarTreeMapper.NAME, field);
        builder.field(StarTreeMapper.TYPE, NUMERIC);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Dimension dimension = (Dimension) o;
        return Objects.equals(field, dimension.field);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field);
    }
}
