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
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * ReadContext is used to encapsulate all data needed by <code>BlobContainer#readBlobAsync</code>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ReadContext {
    private final long blobSize;
    private final List<StreamPartCreator> asyncPartStreams;
    private final String blobChecksum;

    public ReadContext(long blobSize, List<StreamPartCreator> asyncPartStreams, String blobChecksum) {
        this.blobSize = blobSize;
        this.asyncPartStreams = asyncPartStreams;
        this.blobChecksum = blobChecksum;
    }

    public ReadContext(ReadContext readContext) {
        this.blobSize = readContext.blobSize;
        this.asyncPartStreams = readContext.asyncPartStreams;
        this.blobChecksum = readContext.blobChecksum;
    }

    public String getBlobChecksum() {
        return blobChecksum;
    }

    public int getNumberOfParts() {
        return asyncPartStreams.size();
    }

    public long getBlobSize() {
        return blobSize;
    }

    public List<StreamPartCreator> getPartStreams() {
        return asyncPartStreams;
    }

    /**
     * Functional interface defining an instance that can create an async action
     * to create a part of an object represented as an InputStreamContainer.
     *
     * @opensearch.experimental
     */
    @FunctionalInterface
    @ExperimentalApi
    public interface StreamPartCreator extends Supplier<CompletableFuture<InputStreamContainer>> {
        /**
         * Kicks off a async process to start streaming.
         *
         * @return When the returned future is completed, streaming has
         * just begun. Clients must fully consume the resulting stream.
         */
        @Override
        CompletableFuture<InputStreamContainer> get();
    }
}
