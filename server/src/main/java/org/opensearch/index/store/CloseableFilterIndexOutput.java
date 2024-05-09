/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.store.FilterIndexOutput;

import java.io.IOException;

/**
 * FilterIndexOutput which takes in an additional FunctionalInterface as a parameter to perform required operations once the IndexOutput is closed
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CloseableFilterIndexOutput extends FilterIndexOutput {

    /**
     * Functional Interface which takes the name of the file as input on which the required operations are to be performed
     */
    @FunctionalInterface
    public interface OnCloseListener {
        void onClose(String name) throws IOException;
    }

    private final OnCloseListener onCloseListener;
    private final String fileName;

    public CloseableFilterIndexOutput(IndexOutput out, String fileName, OnCloseListener onCloseListener) {
        super("CloseableFilterIndexOutput for file " + fileName, out);
        this.fileName = fileName;
        this.onCloseListener = onCloseListener;
    }

    @Override
    public void close() throws IOException {
        super.close();
        onCloseListener.onClose(fileName);
    }
}
