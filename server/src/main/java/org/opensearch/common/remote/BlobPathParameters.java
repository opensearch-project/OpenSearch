/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.remote;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;

/**
 * Parameters which can be used to construct a blob path
 *
 */
@ExperimentalApi
public class BlobPathParameters {

    private final List<String> pathTokens;
    private final String filePrefix;

    public BlobPathParameters(final List<String> pathTokens, final String filePrefix) {
        this.pathTokens = pathTokens;
        this.filePrefix = filePrefix;
    }

    public List<String> getPathTokens() {
        return pathTokens;
    }

    public String getFilePrefix() {
        return filePrefix;
    }
}
