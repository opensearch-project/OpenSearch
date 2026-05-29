/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Immutable input data for a delete operation, containing the field name, value,
 * and writer generation needed to identify and delete a document.
 *
 * <p>The {@link org.apache.lucene.index.Term} uid is constructed by the deleter
 * implementation from the field name and value provided here.
 *
 * @param fieldName the name of the field used to identify the document (e.g. "_id")
 * @param id the field id identifying the document to delete
 * @param generation the writer generation whose deleter should handle to delete
 * @opensearch.experimental
 */
@ExperimentalApi
public record DeleteInput(String fieldName, String id, long generation) {

    /**
     * Creates a new DeleteInput.
     *
     * @param fieldName the field name (must not be null)
     * @param id the id of document being deleted. (must not be null)
     * @param generation the writer generation
     */
    public DeleteInput {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName must not be null");
        }
        if (id == null || id.isEmpty()) {
            throw new IllegalArgumentException("term must not be null");
        }
    }
}
