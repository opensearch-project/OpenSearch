/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class FileInfos {

    private final Map<DataFormat, WriterFileSet> writerFilesMap;

    public FileInfos() {
        this.writerFilesMap = new HashMap<>();
    }

    public Map<DataFormat, WriterFileSet> getWriterFilesMap() {
        return Collections.unmodifiableMap(writerFilesMap);
    }

    public void putWriterFileSet(DataFormat format, WriterFileSet writerFileSet) {
        writerFilesMap.put(format, writerFileSet);
    }

    public Optional<WriterFileSet> getWriterFileSet(DataFormat format) {
        return Optional.ofNullable(writerFilesMap.get(format));
    }
}
