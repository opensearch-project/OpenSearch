/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.apache.lucene.codecs.experimental;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

/** Stored field format used by plugaable codec */
public class Lucene90CustomStoredFieldsFormat extends StoredFieldsFormat {

    private static final int ZSTD_BLOCK_LENGTH = 10 * 48 * 1024;
    private static final int LZ4_NATIVE_BLOCK_LENGTH = 10 * 8 * 1024;
    private static final int ZSTD_MAX_DOCS_PER_BLOCK = 4096;
    private static final int ZSTD_BLOCK_SHIFT = 10;
    private static final int LZ4_MAX_DOCS_PER_BLOCK = 1024;
    private static final int LZ4_BLOCK_SHIFT = 10;

    private final CompressionMode ZSTD_MODE;
    private final CompressionMode ZSTD_MODE_NO_DICT;
    private final CompressionMode LZ4_MODE;

    private final Lucene92CustomCodec.Mode mode;

    public static final String MODE_KEY = Lucene90CustomStoredFieldsFormat.class.getSimpleName() + ".mode";

    /** default constructor */
    public Lucene90CustomStoredFieldsFormat() {
        this(Lucene92CustomCodec.Mode.LZ4_NATIVE, Lucene92CustomCodec.DEFAULT_COMPRESSION_LEVEL);
    }

    /** Stored fields format with specified compression algo. */
    public Lucene90CustomStoredFieldsFormat(Lucene92CustomCodec.Mode mode, int compressionLevel) {
        this.mode = Objects.requireNonNull(mode);
        ZSTD_MODE = new ZstdCompressionMode(compressionLevel);
        ZSTD_MODE_NO_DICT = new ZstdNoDictCompressionMode(compressionLevel);
        LZ4_MODE = new LZ4CompressionMode();
    }

    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        String value = si.getAttribute(MODE_KEY);
        if (value == null) {
            throw new IllegalStateException("missing value for " + MODE_KEY + " for segment: " + si.name);
        }
        Lucene92CustomCodec.Mode mode = Lucene92CustomCodec.Mode.valueOf(value);
        return impl(mode).fieldsReader(directory, si, fn, context);
    }

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        String previous = si.putAttribute(MODE_KEY, mode.name());
        if (previous != null && previous.equals(mode.name()) == false) {
            throw new IllegalStateException(
                "found existing value for " + MODE_KEY + " for segment: " + si.name + " old = " + previous + ", new = " + mode.name()
            );
        }
        return impl(mode).fieldsWriter(directory, si, context);
    }

    private StoredFieldsFormat impl(Lucene92CustomCodec.Mode mode) {
        switch (mode) {
            case ZSTD:
                return new Lucene90CompressingStoredFieldsFormat(
                    "CustomStoredFieldsZstd",
                    ZSTD_MODE,
                    ZSTD_BLOCK_LENGTH,
                    ZSTD_MAX_DOCS_PER_BLOCK,
                    ZSTD_BLOCK_SHIFT
                );
            case ZSTD_NO_DICT:
                return new Lucene90CompressingStoredFieldsFormat(
                    "CustomStoredFieldsZstdNoDict",
                    ZSTD_MODE_NO_DICT,
                    ZSTD_BLOCK_LENGTH,
                    ZSTD_MAX_DOCS_PER_BLOCK,
                    ZSTD_BLOCK_SHIFT
                );
            case LZ4_NATIVE:
                return new Lucene90CompressingStoredFieldsFormat(
                    "CustomStoredFieldsLz4",
                    LZ4_MODE,
                    LZ4_NATIVE_BLOCK_LENGTH,
                    LZ4_MAX_DOCS_PER_BLOCK,
                    LZ4_BLOCK_SHIFT
                );
            default:
                throw new AssertionError();
        }
    }
}
