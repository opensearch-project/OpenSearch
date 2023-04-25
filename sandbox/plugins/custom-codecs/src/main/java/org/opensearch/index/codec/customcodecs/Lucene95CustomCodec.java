/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;

import java.util.Locale;

abstract class Lucene95CustomCodec extends FilterCodec {
    public static final int DEFAULT_COMPRESSION_LEVEL = 6;

    /** Each mode represents a compression algorithm. */
    public enum Mode {
        ZSTD,
        ZSTDNODICT
    }

    private final StoredFieldsFormat storedFieldsFormat;

    /**
     * Creates a new compression codec with the default compression level.
     *
     * @param mode The compression codec (ZSTD or ZSTDNODICT).
     */
    public Lucene95CustomCodec(Mode mode) {
        this(mode, DEFAULT_COMPRESSION_LEVEL);
    }

    /**
     * Creates a new compression codec with the given compression level. We use
     * lowercase letters when registering the codec so that we remain consistent with
     * the other compression codecs: default, lucene_default, and best_compression.
     *
     * @param mode The compression codec (ZSTD or ZSTDNODICT).
     * @parama compressionLevel The compression level.
     */
    public Lucene95CustomCodec(Mode mode, int compressionLevel) {
        super(mode.name().toLowerCase(Locale.ROOT), new Lucene95Codec());
        this.storedFieldsFormat = new Lucene95CustomStoredFieldsFormat(mode, compressionLevel);
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
