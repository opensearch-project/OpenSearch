/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * The content of a blob along with the opaque version token observed when it was read. The token can be passed to
 * {@link BlobContainer#writeBlobConditionally} to perform a compare-and-swap update of the blob.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class VersionedBlob {

    private final byte[] content;
    private final String versionToken;

    /**
     * @param content      the blob content
     * @param versionToken the opaque version token (e.g. ETag) the blob had when read
     */
    public VersionedBlob(byte[] content, String versionToken) {
        this.content = Objects.requireNonNull(content, "content must not be null");
        this.versionToken = Objects.requireNonNull(versionToken, "versionToken must not be null");
    }

    /** Returns the blob content. */
    public byte[] content() {
        return content;
    }

    /** Returns the opaque version token the blob had when read. */
    public String versionToken() {
        return versionToken;
    }
}
