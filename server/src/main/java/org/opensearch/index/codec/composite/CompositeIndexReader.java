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

import java.io.IOException;
import java.util.List;

/**
 * Interface that abstracts the functionality to read composite index structures from the segment
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CompositeIndexReader {
    /**
     * Get list of composite index fields from the segment
     *
     */
    List<String> getCompositeIndexFields();

    /**
     * Get composite index values based on the field name and the field type
     */
    CompositeIndexValues getCompositeIndexValues(String field, CompositeMappedFieldType.CompositeFieldType fieldType) throws IOException;
}
