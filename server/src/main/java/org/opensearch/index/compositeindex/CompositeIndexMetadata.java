/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.index.mapper.CompositeMappedFieldType;

/**
 * This class represents the metadata of a Composite Index, which includes information about
 * the composite field name, type, and the specific metadata for the type of composite field
 * (e.g., Tree metadata).
 *
 * @opensearch.experimental
 */
public class CompositeIndexMetadata {

    private final String compositeFieldName;
    private final CompositeMappedFieldType.CompositeFieldType compositeFieldType;

    /**
     * Constructs a CompositeIndexMetadata object with the provided composite field name and type.
     *
     * @param compositeFieldName the name of the composite field
     * @param compositeFieldType the type of the composite field
     */
    public CompositeIndexMetadata(String compositeFieldName, CompositeMappedFieldType.CompositeFieldType compositeFieldType) {
        this.compositeFieldName = compositeFieldName;
        this.compositeFieldType = compositeFieldType;
    }

    /**
     * Returns the name of the composite field.
     *
     * @return the composite field name
     */
    public String getCompositeFieldName() {
        return compositeFieldName;
    }

    /**
     * Returns the type of the composite field.
     *
     * @return the composite field type
     */
    public CompositeMappedFieldType.CompositeFieldType getCompositeFieldType() {
        return compositeFieldType;
    }
}
