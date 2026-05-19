/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

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
     * Delimiter used to separate data format and filename in serialized form.
     * Uses "/" to match the subdirectory convention (e.g., "parquet/_0.pqt").
     */
    public static final String DELIMITER = "/";
    private static final String DEFAULT_FORMAT = "lucene";
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
     * The format is "format/file" (e.g., "parquet/_0.pqt"). If no delimiter is present,
     * files starting with "metadata" are treated as metadata format, otherwise defaults to "lucene".
     *
     * @param dataFormatAwareFile the serialized filename with optional data format prefix
     */
    public FileMetadata(String dataFormatAwareFile) {
        int slash = dataFormatAwareFile.indexOf(DELIMITER);
        if (slash >= 0) {
            this.dataFormat = dataFormatAwareFile.substring(0, slash);
            this.file = dataFormatAwareFile.substring(slash + 1);
        } else if (dataFormatAwareFile.startsWith(METADATA_KEY)) {
            this.dataFormat = METADATA_KEY;
            this.file = dataFormatAwareFile;
        } else {
            this.dataFormat = DEFAULT_FORMAT;
            this.file = dataFormatAwareFile;
        }
    }

    /**
     * Serializes a data format and filename into a format-aware string without creating an intermediate object.
     * For the default lucene format, returns just the filename (no prefix).
     *
     * @param dataFormat the data format identifier (e.g., "lucene", "parquet")
     * @param file the filename
     * @return the serialized representation (e.g., "parquet/_0.parquet" or "_0.si" for lucene)
     */
    public static String serialize(String dataFormat, String file) {
        if (DEFAULT_FORMAT.equals(dataFormat)) {
            return file;
        }
        return dataFormat + DELIMITER + file;
    }

    /**
     * Extracts the plain filename from a serialized format-aware filename without creating an intermediate object.
     *
     * @param serialized the serialized filename (e.g., "parquet/_0.parquet" or "_0.si")
     * @return the plain filename (e.g., "_0.parquet" or "_0.si")
     */
    public static String parseFile(String serialized) {
        int slash = serialized.indexOf(DELIMITER);
        return slash >= 0 ? serialized.substring(slash + 1) : serialized;
    }

    /**
     * Extracts the data format from a serialized format-aware filename without creating an intermediate object.
     *
     * @param serialized the serialized filename (e.g., "parquet/_0.parquet" or "_0.si")
     * @return the data format (e.g., "parquet", "lucene", or "metadata")
     */
    public static String parseDataFormat(String serialized) {
        int slash = serialized.indexOf(DELIMITER);
        if (slash >= 0) {
            return serialized.substring(0, slash);
        }
        return serialized.startsWith(METADATA_KEY) ? METADATA_KEY : DEFAULT_FORMAT;
    }

    /**
     * Serializes this FileMetadata to a string in the format "format/file".
     * For the default lucene format, returns just the filename (no prefix).
     *
     * @return the serialized representation
     */
    public String serialize() {
        return serialize(dataFormat, file);
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
