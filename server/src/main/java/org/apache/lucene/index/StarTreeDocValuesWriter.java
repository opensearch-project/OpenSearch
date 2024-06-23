/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.index;

/**
 * A wrapper for {@link DocValuesWriter} that contains the {@link DocValuesType} of the doc
 */
public class StarTreeDocValuesWriter {

    private final DocValuesType docValuesType;
    private final DocValuesWriter<?> docValuesWriter;

    public StarTreeDocValuesWriter(DocValuesType docValuesType, DocValuesWriter docValuesWriter) {
        this.docValuesType = docValuesType;
        this.docValuesWriter = docValuesWriter;
    }

    /**
     * Get the doc values type
     */
    public DocValuesType getDocValuesType() {
        return docValuesType;
    }

    /**
     * Get the doc values writer
     */
    public DocValuesWriter<?> getDocValuesWriter() {
        return docValuesWriter;
    }
}
