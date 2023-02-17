/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene92.Lucene92Codec;

abstract class Lucene92CustomCodec extends FilterCodec {
    public static final int DEFAULT_COMPRESSION_LEVEL = 6;

    /** Each mode represents a compression algorithm. */
    public enum Mode {
        ZSTD,
        ZSTDNODICT
    }

    private final StoredFieldsFormat storedFieldsFormat;

    /** new codec for a given compression algorithm and default compression level */
    public Lucene92CustomCodec(Mode mode) {
        this(mode, DEFAULT_COMPRESSION_LEVEL);
    }

    public Lucene92CustomCodec(Mode mode, int compressionLevel) {
        super(mode.name(), new Lucene92Codec());
        this.storedFieldsFormat = new Lucene92CustomStoredFieldsFormat(mode, compressionLevel);
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
