/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read;

/**
 * ABCDE
 *
 * @opensearch.internal
 */
public class ReadContext {

    private final String fileName;
    private final StreamApplier[] streamAppliers;

    /**
     * ABCDE
     *
     * @param fileName
     * @param streamAppliers
     */
    public ReadContext(String fileName, StreamApplier... streamAppliers) {
        this.fileName = fileName;
        this.streamAppliers = streamAppliers;
    }

    /**
     * ABCDE
     *
     * @return
     */
    public String getFileName() {
        return fileName;
    }

    /**
     * ABCDE
     *
     * @return
     */
    public StreamApplier[] getStreamAppliers() {
        return streamAppliers;
    }
}
