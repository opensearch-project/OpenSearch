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
 * Container for file information organized by data format.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FileInfos {

    private final Map<DataFormat, WriterFileSet> writerFilesMap;

    private FileInfos() {
        this.writerFilesMap = new HashMap<>();
    }

    /**
     * Gets an unmodifiable map of writer file sets by data format.
     *
     * @return the writer files map
     */
    public Map<DataFormat, WriterFileSet> getWriterFilesMap() {
        return Collections.unmodifiableMap(writerFilesMap);
    }

    private void putWriterFileSet(DataFormat format, WriterFileSet writerFileSet) {
        writerFilesMap.put(format, writerFileSet);
    }

    /**
     * Gets the writer file set for a specific data format.
     *
     * @param format the data format
     * @return an Optional containing the writer file set, or empty if not found
     */
    public Optional<WriterFileSet> getWriterFileSet(DataFormat format) {
        return Optional.ofNullable(writerFilesMap.get(format));
    }

    /**
     * Creates an empty FileInfos instance.
     *
     * @return an empty FileInfos
     */
    public static FileInfos empty() {
        return new FileInfos();
    }

    /**
     * Creates a new builder for FileInfos.
     *
     * @return a new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for constructing FileInfos instances.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static final class Builder {
        private final Map<DataFormat, WriterFileSet> writerFilesMap = new HashMap<>();

        /**
         * Adds a writer file set for a specific data format.
         *
         * @param format the data format
         * @param writerFileSet the writer file set
         * @return this builder
         */
        public Builder putWriterFileSet(DataFormat format, WriterFileSet writerFileSet) {
            writerFilesMap.put(format, writerFileSet);
            return this;
        }

        /**
         * Adds all entries from the provided map.
         *
         * @param map the map of data formats to writer file sets
         * @return this builder
         */
        public Builder putAll(Map<DataFormat, WriterFileSet> map) {
            writerFilesMap.putAll(map);
            return this;
        }

        /**
         * Builds the FileInfos instance.
         *
         * @return a new FileInfos instance
         */
        public FileInfos build() {
            FileInfos fileInfos = new FileInfos();
            writerFilesMap.forEach(fileInfos::putWriterFileSet);
            return fileInfos;
        }
    }
}
