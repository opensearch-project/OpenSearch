/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;

/**
 * A {@link BlobContainer} that can exchange blobs with another container entirely on the storage provider's side,
 * without the bytes passing through this process.
 *
 * <p>A container plays one of two roles. As a <em>source</em> it describes itself via {@link #asCopySource()}. As a
 * <em>destination</em> it performs the transfer via {@link #copyBlobFrom}. The transfer is driven from the destination
 * because the provider requires a single principal to hold read on the source and write on the destination at the same
 * time, and when the destination belongs to another account only that account can grant the write half - so the
 * destination's credentials are the ones that must sign.
 *
 * <p>This is a capability interface rather than a default method on {@link BlobContainer} so that callers can
 * feature-detect with {@code instanceof}: most implementations have no provider-side copy primitive, and silently
 * degrading to a read-then-write would defeat the reason this interface exists.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface ServerSideCopyBlobContainer extends BlobContainer {

    /**
     * Describes this container as the source of a server-side copy: its container, the prefix its blobs live under,
     * and the account expected to own it.
     *
     * <p>Does not describe how a reader should reach it. Which endpoint - for example which S3 access point - a given
     * reader is entitled to use is a property of that reader's grant rather than of this container, so the
     * destination supplies it.
     *
     * <p>Implementations must reject a container that is itself configured as a copy <em>destination</em>. Reader
     * endpoint configuration is ignored on a source, so accepting it would silently copy from somewhere the operator
     * did not intend.
     *
     * @throws UnsupportedOperationException if this container cannot act as a server-side copy source
     * @throws IllegalStateException         if this container is configured as a copy destination
     */
    ServerSideCopySource asCopySource();

    /**
     * Copies a blob from {@code source} into this container, entirely on the storage provider's side.
     *
     * <p>Implementations MUST NOT stream the blob through the calling process. If the provider cannot satisfy the
     * request server-side, the implementation must fail rather than fall back to a read-then-write.
     *
     * <p>Reaching a source in another account may require an endpoint this container is specifically granted on.
     * Implementations take that from their own configuration, not from {@code source}.
     *
     * @param source              the container holding the blob, its key prefix and its expected owner
     * @param sourceBlobName      blob name relative to {@code source}'s key prefix
     * @param destinationBlobName name to write within this container
     * @throws java.nio.file.NoSuchFileException if the source blob does not exist
     * @throws IOException                       if the copy cannot be performed
     */
    void copyBlobFrom(ServerSideCopySource source, String sourceBlobName, String destinationBlobName) throws IOException;

    /**
     * Largest blob, in bytes, this implementation can copy in a single request. Larger blobs are copied using whatever
     * chunked mechanism the provider offers.
     */
    long maxSingleRequestCopySizeInBytes();

    /**
     * Whether this container is configured to pull server-side copies. An implementation may be present but
     * unconfigured - lacking the endpoint it needs to reach a foreign source - in which case callers should treat the
     * capability as absent rather than invoking it and handling the failure.
     */
    default boolean isServerSideCopyConfigured() {
        return true;
    }
}
