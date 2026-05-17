/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableFieldType;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

/**
 * Strategy for creating Lucene fields from a {@link MappedFieldType} and value.
 * Each implementation handles one OpenSearch field type (e.g., text, keyword).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
@FunctionalInterface
public interface LuceneFieldFactory {

    /**
     * Adds the appropriate Lucene field(s) to the document for the given mapped field type and value.
     *
     * @param document  the Lucene document to add fields to
     * @param fieldType the OpenSearch mapped field type (carries name, analyzer, doc-values config)
     * @param value     the field value
     */
    void addField(Document document, MappedFieldType fieldType, Object value, IndexableFieldType luceneFieldType);
}
