/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import reactor.util.annotation.NonNull;

public record FileMetadata(String dataFormat, String directory, String file) {
    @Override
    public @NonNull String toString() {
        return "FileMetadata {" + "directory='" + directory + '\'' + ", file='" + file + '\'' + ", format='" + dataFormat + '\'' + '}';
    }
}
