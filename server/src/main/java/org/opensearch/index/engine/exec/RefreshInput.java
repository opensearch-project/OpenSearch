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

    private final List<WriterFileSet> writerFiles;
    private final List<WriterFileSet> filesToRemove;

    public RefreshInput() {
        this.writerFiles = new ArrayList<>();
        this.filesToRemove = new ArrayList<>();
    }

    public void add(WriterFileSet writerFileSetGroup) {
        this.writerFiles.add(writerFileSetGroup);
    }

    public void addFilesToRemove(WriterFileSet writerFileSetGroup) {
        this.filesToRemove.add(writerFileSetGroup);
    }

    public List<WriterFileSet> getWriterFiles() {
        return writerFiles;
    }

    public List<WriterFileSet> getFilesToRemove() {
        return filesToRemove;
    }
}
