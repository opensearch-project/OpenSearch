/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Interface for format-specific store directories that handle file operations
 * for different data formats (Lucene, Parquet, etc.)
 * This interface is format-agnostic and does not depend on Lucene's Directory class.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public interface FormatStoreDirectory<T extends DataFormat> extends Closeable {

    /**
     * Returns the data format this directory handles
     * @return the data format for this directory
     */
    T getDataFormat();

    /**
     * Checks if this directory can handle the given DataFormat
     * @param format the DataFormat to check
     * @return true if this directory supports the format, false otherwise
     */
    default boolean supportsFormat(DataFormat format) {
        return getDataFormat().equals(format);
    }

    /**
     * Determines if this directory can handle the given file
     * @param fileName the name of the file to check
     * @return true if this directory accepts the file, false otherwise
     * @deprecated Use format-based routing with FileMetadata instead of file extension detection.
     *             This method will be removed in a future version.
     */
    @Deprecated
    boolean acceptsFile(String fileName);

    /**
     * Returns the directory path for this format
     * @return the path to this format's directory
     */
    Path getDirectoryPath();

    /**
     * Performs format-specific initialization
     * @throws IOException if initialization fails
     */
    void initialize() throws IOException;

    /**
     * Performs format-specific cleanup
     * @throws IOException if cleanup fails
     */
    void cleanup() throws IOException;

    // Format-agnostic file operations using standard Java I/O

    /**
     * Lists all files in this directory
     * @return array of file names in the directory
     * @throws IOException if listing fails
     */
    FileMetadata[] listAll() throws IOException;

    /**
     * Deletes the specified file
     * @param name the name of the file to delete
     * @throws IOException if deletion fails
     */
    void deleteFile(String name) throws IOException;

    /**
     * Returns the length of the specified file
     * @param name the name of the file
     * @return the length of the file in bytes
     * @throws IOException if the file cannot be accessed
     */
    long fileLength(String name) throws IOException;

    /**
     * Creates an output stream for writing to the specified file
     * @param name the name of the file to create
     * @return an OutputStream for writing to the file
     * @throws IOException if the output stream cannot be created
     */
    OutputStream createOutput(String name) throws IOException;

    /**
     * Opens an input stream for reading from the specified file
     * @param name the name of the file to read
     * @return an InputStream for reading from the file
     * @throws IOException if the input stream cannot be opened
     */
    InputStream openInput(String name) throws IOException;

    /**
     * Syncs the specified files to persistent storage
     * @param names collection of file names to sync
     * @throws IOException if syncing fails
     */
    void sync(Collection<String> names) throws IOException;

    /**
     * Syncs metadata to persistent storage
     * @throws IOException if metadata sync fails
     */
    void syncMetaData() throws IOException;

    /**
     * Renames a file
     * @param source the current name of the file
     * @param dest the new name for the file
     * @throws IOException if renaming fails
     */
    void rename(String source, String dest) throws IOException;

    /**
     * Checks if a file exists
     * @param name the name of the file to check
     * @return true if the file exists, false otherwise
     * @throws IOException if the existence check fails
     */
    boolean fileExists(String name) throws IOException;

    /**
     * Calculates the checksum for the specified file using format-specific method
     * @param fileName the name of the file to calculate checksum for
     * @return the checksum as a string representation
     * @throws IOException if checksum calculation fails
     */
    long calculateChecksum(String fileName) throws IOException;

    // Upload-specific methods for remote segment upload

    /**
     * Creates an input stream for reading complete file content during upload operations.
     * This method provides format-agnostic access to file content for remote upload.
     *
     * @param fileName the name of the file to read
     * @return InputStream for reading the complete file content
     * @throws IOException if the input stream cannot be created or the file does not exist
     */
    InputStream createUploadInputStream(String fileName) throws IOException;

    /**
     * Creates a range-based input stream for multi-part upload operations.
     * This method enables efficient multi-stream uploads by providing access to specific
     * byte ranges within a file, supporting parallel upload of large files.
     *
     * @param fileName the name of the file to read
     * @param offset the starting byte offset within the file (0-based)
     * @param length the number of bytes to read from the offset
     * @return InputStream for reading the specified byte range
     * @throws IOException if the range stream cannot be created, the file does not exist,
     *                     or the offset/length parameters are invalid
     * @throws IllegalArgumentException if offset is negative, length is negative or zero,
     *                                  or offset + length exceeds file size
     */
    InputStream createUploadRangeInputStream(String fileName, long offset, long length) throws IOException;

    /**
     * Calculates format-specific checksum for upload verification.
     * This method provides format-appropriate checksum calculation that may differ
     * from the general calculateChecksum method. For example, Lucene files use
     * checksum-of-checksum calculation, while other formats may use SHA-256.
     *
     * @param fileName the name of the file to calculate checksum for
     * @return checksum string in format-specific representation suitable for upload verification
     * @throws IOException if checksum calculation fails or the file cannot be accessed
     */
    String calculateUploadChecksum(String fileName) throws IOException;

    /**
     * Performs format-specific post-upload operations after successful file upload.
     * This method allows format implementations to execute any necessary cleanup,
     * logging, or state updates after a file has been successfully uploaded to remote storage.
     *
     * @param fileName the name of the local file that was uploaded
     * @param remoteFileName the name/path of the file in remote storage
     * @throws IOException if post-upload operations fail
     */
    void onUploadComplete(String fileName, String remoteFileName) throws IOException;

    /**
     * Opens an IndexInput for reading from the specified file.
     * This method mirrors Lucene Directory.openInput() behavior, providing
     * full random access, seeking, and cloning capabilities for compatibility
     * with existing Lucene-based code.
     *
     * @param name the name of the file to read
     * @param context IOContext providing performance hints for the operation
     * @return IndexInput for reading from the file with full Lucene compatibility
     * @throws IOException if the IndexInput cannot be created or file does not exist
     */
    IndexInput openIndexInput(String name, IOContext context) throws IOException;
}
