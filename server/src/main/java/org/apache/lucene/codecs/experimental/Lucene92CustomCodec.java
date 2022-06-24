/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.codecs.experimental;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene92.Lucene92Codec;

/** Custom codec for different compression algorithm */
public class Lucene92CustomCodec extends FilterCodec {

    public static final int DEFAULT_COMPRESSION_LEVEL = 6;

    private static final int ZSTD_MIN_CLEVEL = -(1 << 17);
    private static final int ZSTD_MAX_CLEVEL = 22;

    private final StoredFieldsFormat storedFieldsFormat;

    /** Compression modes */
    public static enum Mode {
        // Zstandard with dictionary
        ZSTD,
        // Zstandard without dictionary
        ZSTD_NO_DICT,
        // lz4 native
        LZ4_NATIVE
    }

    /** Default codec */
    public Lucene92CustomCodec() {
        this(Mode.LZ4_NATIVE, DEFAULT_COMPRESSION_LEVEL);
    }

    /** new codec for a given compression algorithm and default compression level */
    public Lucene92CustomCodec(Mode compressionMode) {
        this(compressionMode, DEFAULT_COMPRESSION_LEVEL);
    }

    /** new codec for a given compression algorithm and compression level */
    public Lucene92CustomCodec(Mode compressionMode, int compressionLevel) {
        super("Lucene92CustomCodec", new Lucene92Codec());

        switch (compressionMode) {
            case ZSTD:
                if (compressionLevel < ZSTD_MIN_CLEVEL || compressionLevel > ZSTD_MAX_CLEVEL) throw new IllegalArgumentException(
                    "Invalid compression level"
                );

                this.storedFieldsFormat = new Lucene92CustomStoredFieldsFormat(Mode.ZSTD, compressionLevel);
                break;
            case ZSTD_NO_DICT:
                if (compressionLevel < ZSTD_MIN_CLEVEL || compressionLevel > ZSTD_MAX_CLEVEL) throw new IllegalArgumentException(
                    "Invalid compression level"
                );

                this.storedFieldsFormat = new Lucene92CustomStoredFieldsFormat(Mode.ZSTD_NO_DICT, compressionLevel);
                break;
            case LZ4_NATIVE:
                this.storedFieldsFormat = new Lucene92CustomStoredFieldsFormat(Mode.LZ4_NATIVE, compressionLevel);
                break;
            default:
                throw new IllegalArgumentException("Chosen compression mode does not exist");
        }
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
