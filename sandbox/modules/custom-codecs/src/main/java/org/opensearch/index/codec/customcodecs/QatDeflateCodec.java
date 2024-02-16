/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

/**
 * QatDeflateCodec provides a compressor using the <a
 * href="https://github.com/intel/qat-java">qat-java</a> library.
 */
public class QatDeflateCodec extends Lucene99QatCodec {

    /** default constructor */
    public QatDeflateCodec() {
        this(DEFAULT_COMPRESSION_LEVEL, "hardware");
    }

    /**
     * Creates a new QatDeflateCodec instance with the default compression level.
     *
     * @param accelerationMode The acceleration mode.
     */
    public QatDeflateCodec(String accelerationMode) {
        this(DEFAULT_COMPRESSION_LEVEL, accelerationMode);
    }

    /**
     * Creates a new QatDeflateCodec instance.
     *
     * @param compressionLevel The compression level.
     * @param accelerationMode The acceleration mode.
     */
    public QatDeflateCodec(int compressionLevel, String accelerationMode) {
        super(Mode.QDEFLATE, compressionLevel, accelerationMode);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
