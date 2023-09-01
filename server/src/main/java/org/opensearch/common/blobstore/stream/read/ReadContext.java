/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.io.InputStreamContainer;

import java.util.List;

/**
 * ReadContext is used to encapsulate all data needed by <code>BlobContainer#readBlobAsync</code>
 */
@ExperimentalApi
public class ReadContext {
    private final long blobSize;
    private final List<InputStreamContainer> partStreams;
    private final String blobChecksum;

    public ReadContext(long blobSize, List<InputStreamContainer> partStreams, String blobChecksum) {
        this.blobSize = blobSize;
        this.partStreams = partStreams;
        this.blobChecksum = blobChecksum;
    }

    public String getBlobChecksum() {
        return blobChecksum;
    }

    public int getNumberOfParts() {
        return partStreams.size();
    }

    public long getBlobSize() {
        return blobSize;
    }

    public List<InputStreamContainer> getPartStreams() {
        return partStreams;
    }
}
