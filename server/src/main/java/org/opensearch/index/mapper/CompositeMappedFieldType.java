/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Base class for composite field types
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public abstract class CompositeMappedFieldType extends MappedFieldType {
    private final List<String> fields;
    private final CompositeFieldType type;

    public CompositeMappedFieldType(
        String name,
        boolean isIndexed,
        boolean isStored,
        boolean hasDocValues,
        TextSearchInfo textSearchInfo,
        Map<String, String> meta,
        List<String> fields,
        CompositeFieldType type
    ) {
        super(name, isIndexed, isStored, hasDocValues, textSearchInfo, meta);
        this.fields = fields;
        this.type = type;
    }

    public CompositeMappedFieldType(String name, List<String> fields, CompositeFieldType type) {
        this(name, false, false, false, TextSearchInfo.NONE, Collections.emptyMap(), fields, type);
    }

    /**
     * Supported composite field types
     */
    public enum CompositeFieldType {
        STAR_TREE
    }

    public List<String> fields() {
        return fields;
    }
}
