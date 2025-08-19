/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.util.ArrayList;
import java.util.List;

public class RefreshInput {

    private List<FileMetadata> files;

    public RefreshInput() {
        this.files = new ArrayList<>();
    }

    public void add(FileMetadata fileMetadata) {
        this.files.add(fileMetadata);
    }

    public List<FileMetadata> getFiles() {
        return files;
    }
}
