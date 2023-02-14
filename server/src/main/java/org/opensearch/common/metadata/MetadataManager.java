/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.metadata;

import java.io.IOException;
import java.util.Map;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

/**
 * Manages Metadata by adding version, codec in header and checksum in footer.
 * @param <T> The type of metadata to be read/written
 */
public class MetadataManager<T> {
    // This can be updated to hold a parserFactory and get relevant parsers based on the metadata versions
    private final MetadataParser<T> parser;
    private final int currentVersion;
    private final String codec;

    /**
     * @param parser parser to read/write metadata from T
     * @param currentVersion latest supported version of the metadata
     * @param codec: metadata codec
     */
    public MetadataManager(MetadataParser<T> parser, int currentVersion, String codec) {
        this.parser = parser;
        this.currentVersion = currentVersion;
        this.codec = codec;
    }

    /**
     * Reads metadata content from {@code indexInput} and parses the read content to {@link T}.
     * Before reading actual content, verifies the header with relevant codec and version.
     * After reading the actual content, verifies the checksum as well
     * @param indexInput metadata file input stream
     * @return metadata content parsed into {@link T}
     */
    public T readMetadata(IndexInput indexInput) throws IOException {
        ChecksumIndexInput checksumIndexInput = new BufferedChecksumIndexInput(indexInput);
        checkHeader(checksumIndexInput);
        T metadata = this.parser.readContent(checksumIndexInput);
        checkFooter(checksumIndexInput);
        return metadata;
    }

    /**
     * Writes metadata to file output stream {@code indexOutput}
     * @param indexOutput file output stream which will store metadata content
     * @param metadata metadata content.
     *
     * {@code metadata} argument type would be changed to {@link T} in future to reuse {@link MetadataManager} for multiple metadata types
     */
    public void writeMetadata(IndexOutput indexOutput, Map<String, String> metadata) throws IOException {
        this.writeHeader(indexOutput);
        this.parser.writeContent(indexOutput, metadata);
        this.writeFooter(indexOutput);
    }

    /**
     * Reads header from metadata file input stream containing {@code this.codec} and {@code this.currentVersion}.
     * @param indexInput metadata file input stream
     * @return header version found in the metadata file
     */
    private int checkHeader(IndexInput indexInput) throws IOException {
        return CodecUtil.checkHeader(indexInput, this.codec, this.currentVersion, this.currentVersion);
    }

    /**
     * Reads footer from metadata file input stream containing checksum.
     * The {@link IndexInput#getFilePointer()} should be at the footer start position.
     * @param indexInput metadata file input stream
     */
    private void checkFooter(ChecksumIndexInput indexInput) throws IOException {
        CodecUtil.checkFooter(indexInput);
    }

    /**
     * Writes header with {@code this.codec} and {@code this.currentVersion} to the metadata file output stream
     * @param indexOutput metadata file output stream
     */
    private void writeHeader(IndexOutput indexOutput) throws IOException {
        CodecUtil.writeHeader(indexOutput, this.codec, this.currentVersion);
    }

    /**
     * Writes footer with checksum of contents of metadata file output stream
     * @param indexOutput metadata file output stream
     */
    private void writeFooter(IndexOutput indexOutput) throws IOException {
        CodecUtil.writeFooter(indexOutput);
    }
}
