/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

/**
 * QatLz4Codec provides a compressor using the <a
 * href="https://github.com/intel/qat-java">qat-java</a> library.
 */
public class QatLz4Codec extends Lucene99QatCodec {

    /** default constructor */
    public QatLz4Codec() {
        this(DEFAULT_COMPRESSION_LEVEL, "hardware");
    }

    /**
     * Creates a new QatLz4Codec instance with the default compression level.
     * @param accelerationMode The acceleration mode.
     */
    public QatLz4Codec(String accelerationMode) {
        this(DEFAULT_COMPRESSION_LEVEL, accelerationMode);
    }

    /**
     * Creates a new QatLz4Codec instance.
     *
     * @param compressionLevel The compression level.
     * @param accelerationMode The acceleration mode.
     */
    public QatLz4Codec(int compressionLevel, String accelerationMode) {
        super(Mode.QLZ4, compressionLevel, accelerationMode);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
