/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.index.engine.exec.FileMetadata;

import java.util.Collection;

public class CatalogSnapshotMetadata {
    Collection<FileMetadata> files;
    String path;

    public Collection<FileMetadata> getFiles() {
        return files;
    }

    public String getPath() {
        return path;
    }
}
