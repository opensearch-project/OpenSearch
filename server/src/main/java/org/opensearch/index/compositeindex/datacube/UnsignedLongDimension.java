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

/**
 * Unsigned Long dimension class
 *
 * @opensearch.experimental
 */
public class UnsignedLongDimension extends NumericDimension {

    public static final String UNSIGNED_LONG = "unsigned_long";

    public UnsignedLongDimension(String field) {
        super(field);
    }

    @Override
    public DimensionDataType getDimensionDataType() {
        return DimensionDataType.UNSIGNED_LONG;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CompositeDataCubeFieldType.NAME, getField());
        builder.field(CompositeDataCubeFieldType.TYPE, UNSIGNED_LONG);
        builder.endObject();
        return builder;
    }

}
