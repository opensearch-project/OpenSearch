/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Composite index dimension base class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Dimension implements ToXContent {
    private final String field;

    public Dimension(String field) {
        this.field = field;
    }

    public String getField() {
        return field;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(field);
        builder.field("type", "numeric");
        builder.endObject();
        return builder;
    }
}
