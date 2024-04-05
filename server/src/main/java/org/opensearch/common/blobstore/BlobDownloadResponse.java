/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import java.io.InputStream;
import java.util.Map;

/**
 * Represents the response from a blob download operation, containing both the
 * input stream of the blob content and the associated metadata.
 *
 * @opensearch.experimental
 */
public class BlobDownloadResponse {

    /**
     * Downloaded blob InputStream
     */
    private final InputStream inputStream;

    /**
     * Metadata of the downloaded blob
     */
    private final Map<String, String> metadata;

    public InputStream getInputStream() {
        return inputStream;
    }

    public Map<String, String> getMetadata() {
        return metadata;
    }

    public BlobDownloadResponse(InputStream inputStream, Map<String, String> metadata) {
        this.inputStream = inputStream;
        this.metadata = metadata;
    }

}
