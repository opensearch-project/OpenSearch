/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.Strings;

import java.util.Objects;

/**
 * Addresses the source of a server-side blob copy.
 *
 * <p>Produced by {@link ServerSideCopyBlobContainer#asCopySource()} on the container holding the data, and consumed by
 * {@link ServerSideCopyBlobContainer#copyBlobFrom} on the container receiving it. The copy is therefore driven from
 * the destination side, because the single principal signing it must hold write on the destination - and for a
 * destination owned by another account, only that account can grant it.
 *
 * <p>Names the container the data sits in. It does not decide how a reader reaches it: an implementation may be
 * granted on an alternative endpoint over that container - an S3 access point, say - and if so that endpoint is a
 * property of the reader's grant rather than of this source, so the destination supplies it and may address it in
 * place of {@link #container()}. What a destination always takes from here is {@link #keyPrefix()} and
 * {@link #expectedContainerOwner()}.
 *
 * <p>Carries no provider identity, so nothing here prevents a destination being handed a source produced by a
 * different provider's container. Implementations of {@link ServerSideCopyBlobContainer#copyBlobFrom} are expected
 * to reject a source they cannot address.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class ServerSideCopySource {

    private final String container;
    private final String keyPrefix;
    @Nullable
    private final String expectedContainerOwner;

    /**
     * @param container              source container, or an alternative endpoint over it
     * @param keyPrefix              prefix the source container's blobs live under; may be empty
     * @param expectedContainerOwner account expected to own the source container, or {@code null} to skip the
     *                               ownership precondition
     */
    public ServerSideCopySource(final String container, final String keyPrefix, @Nullable final String expectedContainerOwner) {
        if (Strings.hasText(container) == false) {
            throw new IllegalArgumentException("source container must be provided");
        }
        this.container = container;
        this.keyPrefix = keyPrefix == null ? "" : keyPrefix;
        this.expectedContainerOwner = expectedContainerOwner;
    }

    public String container() {
        return container;
    }

    public String keyPrefix() {
        return keyPrefix;
    }

    @Nullable
    public String expectedContainerOwner() {
        return expectedContainerOwner;
    }

    /**
     * Resolves the full source key for a blob name.
     */
    public String resolveKey(final String blobName) {
        if (keyPrefix.isEmpty()) {
            return blobName;
        }
        return keyPrefix.endsWith("/") ? keyPrefix + blobName : keyPrefix + "/" + blobName;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ServerSideCopySource that = (ServerSideCopySource) o;
        return container.equals(that.container)
            && keyPrefix.equals(that.keyPrefix)
            && Objects.equals(expectedContainerOwner, that.expectedContainerOwner);
    }

    @Override
    public int hashCode() {
        return Objects.hash(container, keyPrefix, expectedContainerOwner);
    }

    @Override
    public String toString() {
        return "ServerSideCopySource{container=" + container + ", keyPrefix=" + keyPrefix + "}";
    }
}
