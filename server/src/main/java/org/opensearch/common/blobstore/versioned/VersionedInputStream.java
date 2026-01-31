/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.versioned;

import org.opensearch.common.Nullable;

import java.io.InputStream;

/**
 * Versioned input stream that holds version information along with blob content.
 *
 * @opensearch.internal
 */
public class VersionedInputStream {
    private final String version;
    private final InputStream inputStream;

    /**
     * Constructor for write operations (version only)
     */
    public VersionedInputStream(String version) {
        this.version = version;
        this.inputStream = null;
    }

    /**
     * Constructor for read operations (version + InputStream)
     */
    public VersionedInputStream(String version, InputStream inputStream) {
        this.version = version;
        this.inputStream = inputStream;
    }

    public String getVersion() {
        return version;
    }

    @Nullable
    public InputStream getInputStream() {
        return inputStream;
    }

    public boolean hasInputStream() {
        return inputStream != null;
    }
}