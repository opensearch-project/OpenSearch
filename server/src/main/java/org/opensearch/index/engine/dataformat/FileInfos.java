/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FileInfos {

    private final Map<DataFormat, WriterFileSet> writerFilesMap;

    private FileInfos(Map<DataFormat, WriterFileSet> map) {
        this.writerFilesMap = map;
    }

    public Map<DataFormat, WriterFileSet> writerFilesMap() {
        return Collections.unmodifiableMap(writerFilesMap);
    }

    public Optional<WriterFileSet> getWriterFileSet(DataFormat format) {
        return Optional.ofNullable(writerFilesMap.get(format));
    }

    public static FileInfos empty() {
        return new FileInfos(Map.of());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private final Map<DataFormat, WriterFileSet> map = new HashMap<>();

        public Builder putWriterFileSet(DataFormat format, WriterFileSet writerFileSet) {
            map.put(format, writerFileSet);
            return this;
        }

        public FileInfos build() {
            return new FileInfos(new HashMap<>(map));
        }
    }
}
