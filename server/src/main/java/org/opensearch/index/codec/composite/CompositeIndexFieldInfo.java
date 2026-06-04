
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.CompositeMappedFieldType;

/**
 * Field info details of composite index fields
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeIndexFieldInfo {
    private final String field;
    private final CompositeMappedFieldType.CompositeFieldType type;

    public CompositeIndexFieldInfo(String field, CompositeMappedFieldType.CompositeFieldType type) {
        this.field = field;
        this.type = type;
    }

    public String getField() {
        return field;
    }

    public CompositeMappedFieldType.CompositeFieldType getType() {
        return type;
    }
}
