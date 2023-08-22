/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read;

import org.opensearch.common.CheckedTriFunction;
import org.opensearch.common.StreamContext;
import org.opensearch.common.io.InputStreamContainer;

import java.io.IOException;

/**
 * ReadContext is used to encapsulate all data needed by <code>BlobContainer#readStreams</code>
 *
 * @opensearch.internal
 */
public class ReadContext extends StreamContext {
    private final String blobChecksum;

    public ReadContext(
        CheckedTriFunction<Integer, Long, Long, InputStreamContainer, IOException> streamSupplier,
        long partSize,
        long lastPartSize,
        int numberOfParts,
        String blobChecksum
    ) {
        super(streamSupplier, partSize, lastPartSize, numberOfParts);
        this.blobChecksum = blobChecksum;
    }

    public String getBlobChecksum() {
        return blobChecksum;
    }
}
