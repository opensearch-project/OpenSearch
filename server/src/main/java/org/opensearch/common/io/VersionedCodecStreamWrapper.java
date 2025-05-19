/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;

/**
 * Manages versioning and checksum for a stream of content.
 * @param <T> Type of content to be read/written
 *
 * @opensearch.internal
 */
public class VersionedCodecStreamWrapper<T> {
    private static final Logger logger = LogManager.getLogger(VersionedCodecStreamWrapper.class);

    private final IndexIOStreamHandlerFactory<T> indexIOStreamHandlerFactory;
    private final int minVersion;
    private final int currentVersion;
    private final String codec;

    /**
     * @param indexIOStreamHandlerFactory factory for providing handler to read/write stream from T
     * @param minVersion earliest supported version of the stream
     * @param currentVersion latest supported version of the stream
     * @param codec: stream codec
     */
    public VersionedCodecStreamWrapper(
        IndexIOStreamHandlerFactory<T> indexIOStreamHandlerFactory,
        int minVersion,
        int currentVersion,
        String codec
    ) {
        this.indexIOStreamHandlerFactory = indexIOStreamHandlerFactory;
        this.minVersion = minVersion;
        this.currentVersion = currentVersion;
        this.codec = codec;
    }

    /**
     * Reads stream content from {@code indexInput} and parses the read content to {@link T}.
     * Before reading actual content, verifies the header with relevant codec and version.
     * After reading the actual content, verifies the checksum as well
     * @param indexInput file input stream
     * @return stream content parsed into {@link T}
     */
    public T readStream(IndexInput indexInput) throws IOException {
        logger.debug("Reading input stream [{}] of length - [{}]", indexInput.toString(), indexInput.length());
        try {
            CodecUtil.checksumEntireFile(indexInput);
            int readStreamVersion = checkHeader(indexInput);
            return getHandlerForVersion(readStreamVersion).readContent(indexInput);
        } catch (CorruptIndexException cie) {
            logger.error(
                () -> new ParameterizedMessage(
                    "Error while validating header/footer for [{}]. Total data length [{}]",
                    indexInput.toString(),
                    indexInput.length()
                )
            );
            throw cie;
        }
    }

    /**
     * Writes to file output stream {@code indexOutput}
     * @param indexOutput file output stream which will store stream content
     * @param content stream content.
     */
    public void writeStream(IndexOutput indexOutput, T content) throws IOException {
        this.writeHeader(indexOutput);
        getHandlerForVersion(this.currentVersion).writeContent(indexOutput, content);
        this.writeFooter(indexOutput);
    }

    /**
     * Reads header from file input stream containing {@code this.codec} and {@code this.currentVersion}.
     * @param indexInput file input stream
     * @return header version found in the input stream
     */
    private int checkHeader(IndexInput indexInput) throws IOException {
        // TODO Once versioning strategy is decided we'll add support for min/max supported versions
        return CodecUtil.checkHeader(indexInput, this.codec, minVersion, this.currentVersion);
    }

    /**
     * Reads footer from file input stream containing checksum.
     * The {@link IndexInput#getFilePointer()} should be at the footer start position.
     * @param indexInput file input stream
     */
    private void checkFooter(ChecksumIndexInput indexInput) throws IOException {
        CodecUtil.checkFooter(indexInput);
    }

    /**
     * Writes header with {@code this.codec} and {@code this.currentVersion} to the file output stream
     * @param indexOutput file output stream
     */
    private void writeHeader(IndexOutput indexOutput) throws IOException {
        CodecUtil.writeHeader(indexOutput, this.codec, this.currentVersion);
    }

    /**
     * Writes footer with checksum of contents of file output stream
     * @param indexOutput file output stream
     */
    private void writeFooter(IndexOutput indexOutput) throws IOException {
        CodecUtil.writeFooter(indexOutput);
    }

    /**
     * Returns relevant handler for the version
     * @param version stream content version
     */
    private IndexIOStreamHandler<T> getHandlerForVersion(int version) {
        return this.indexIOStreamHandlerFactory.getHandler(version);
    }
}
