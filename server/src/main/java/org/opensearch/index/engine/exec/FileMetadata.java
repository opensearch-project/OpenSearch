/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * Represents metadata for a file in the index, including its data format and filename.
 * Files can be in different formats (e.g., "lucene", "metadata") and this class provides
 * a unified way to represent and serialize file information across the system.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class FileMetadata {

    /**
     * Delimiter used to separate filename and data format in serialized form.
     */
    public static final String DELIMITER = ":::";
    private static final String METADATA_KEY = "metadata";

    private final String file;
    private final String dataFormat;

    /**
     * Constructs a FileMetadata with explicit data format and filename.
     *
     * @param dataFormat the data format identifier (e.g., "lucene", "metadata")
     * @param file the filename
     */
    public FileMetadata(String dataFormat, String file) {
        this.file = file;
        this.dataFormat = dataFormat;
    }

    /**
     * Constructs a FileMetadata by parsing a serialized data-format-aware filename.
     * The format is "filename:::dataFormat". If no delimiter is present and the filename
     * starts with "metadata", it's treated as a metadata file. Otherwise, defaults to "lucene".
     *
     * @param dataFormatAwareFile the serialized filename with optional data format
     */
    public FileMetadata(String dataFormatAwareFile) {
        if (!dataFormatAwareFile.contains(DELIMITER) && dataFormatAwareFile.startsWith(METADATA_KEY)) {
            this.dataFormat = "metadata";
            this.file = dataFormatAwareFile;
            return;
        }
        String[] parts = dataFormatAwareFile.split(DELIMITER);
        this.dataFormat = (parts.length == 1) ? "lucene" : parts[1];
        this.file = parts[0];
    }

    /**
     * Serializes this FileMetadata to a string in the format "filename:::dataFormat".
     *
     * @return the serialized representation
     */
    public String serialize() {
        return file + DELIMITER + dataFormat;
    }

    @Override
    public String toString() {
        return serialize();
    }

    /**
     * Returns the filename.
     *
     * @return the filename
     */
    public String file() {
        return file;
    }

    /**
     * Returns the data format identifier.
     *
     * @return the data format (e.g., "lucene", "metadata")
     */
    public String dataFormat() {
        return dataFormat;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        FileMetadata that = (FileMetadata) o;
        return Objects.equals(file, that.file) && Objects.equals(dataFormat, that.dataFormat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(file, dataFormat);
    }
}
