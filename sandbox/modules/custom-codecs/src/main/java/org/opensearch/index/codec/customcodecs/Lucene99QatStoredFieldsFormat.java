/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.Objects;

/** Stored field format used by pluggable codec */
public class Lucene99QatStoredFieldsFormat extends StoredFieldsFormat {

    /** A key that we use to map to a mode */
    public static final String MODE_KEY = Lucene99QatStoredFieldsFormat.class.getSimpleName() + ".mode";

    private static final int DEFLATE_BLOCK_LENGTH = 10 * 48 * 1024;
    private static final int DEFLATE_MAX_DOCS_PER_BLOCK = 4096;
    private static final int LZ4_BLOCK_LENGTH = 10 * 8 * 1024;
    private static final int LZ4_MAX_DOCS_PER_BLOCK = 4096;
    private static final int BLOCK_SHIFT = 10;

    private final CompressionMode qatDeflateMode;
    private final CompressionMode qatLz4Mode;
    private final Lucene99QatCodec.Mode mode;

    /** default constructor */
    /*public Lucene99QatStoredFieldsFormat() {
        this(Lucene99QatCodec.Mode.QDEFLATE, Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL);
    }*/

    /**
     * Creates a new instance.
     *
     * @param mode The mode represents QDEFLATE or QLZ4
     */
    /*public Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode mode) {
        this(mode, Lucene99QatCodec.DEFAULT_COMPRESSION_LEVEL);
    }*/

    /**
     * Creates a new instance with the specified mode and compression level.
     *
     * @param mode The mode represents QDEFLATE or QLZ4
     * @param compressionLevel The compression level for the mode.
     */
    /*public Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode mode, int compressionLevel) {
        this.mode = Objects.requireNonNull(mode);
        qatDeflateMode = new QatDeflateMode(compressionLevel);
        qatLz4Mode = new QatLz4Mode(compressionLevel);
    }*/

    /**
     * Creates a new instance with the specified mode and compression level.
     *
     * @param mode The mode represents QDEFLATE or QLZ4
     * @param compressionLevel The compression level for the mode.
     * @param accelerationMode The acceleration mode.
     */
    public Lucene99QatStoredFieldsFormat(Lucene99QatCodec.Mode mode, int compressionLevel, String accelerationMode) {
        this.mode = Objects.requireNonNull(mode);
        qatDeflateMode = new QatDeflateMode(compressionLevel, accelerationMode);
        qatLz4Mode = new QatLz4Mode(compressionLevel, accelerationMode);
    }

    /**
     * Returns a {@link StoredFieldsReader} to load stored fields.
     * @param directory The index directory.
     * @param si The SegmentInfo that stores segment information.
     * @param fn The fieldInfos.
     * @param context The IOContext that holds additional details on the
     *     merge/search context.
     */
    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        String value = si.getAttribute(MODE_KEY);
        if (value == null) {
            throw new IllegalStateException("missing value for " + MODE_KEY + " for segment: " + si.name);
        }
        Lucene99QatCodec.Mode mode = Lucene99QatCodec.Mode.valueOf(value);
        return impl(mode).fieldsReader(directory, si, fn, context);
    }

    /**
     * Returns a {@link StoredFieldsReader} to write stored fields.
     * @param directory The index directory.
     * @param si The SegmentInfo that stores segment information.
     * @param context The IOContext that holds additional details on the
     *     merge/search context.
     */
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

    private StoredFieldsFormat impl(Lucene99QatCodec.Mode mode) {
        switch (mode) {
            case QDEFLATE:
                return new Lucene90CompressingStoredFieldsFormat(
                    "CustomStoredFieldsQatDeflate",
                    qatDeflateMode,
                    DEFLATE_BLOCK_LENGTH,
                    DEFLATE_MAX_DOCS_PER_BLOCK,
                    BLOCK_SHIFT
                );

            case QLZ4:
                return new Lucene90CompressingStoredFieldsFormat(
                    "CustomStoredFieldsQatLz4",
                    qatLz4Mode,
                    LZ4_BLOCK_LENGTH,
                    LZ4_MAX_DOCS_PER_BLOCK,
                    BLOCK_SHIFT
                );
            default:
                throw new AssertionError();
        }
    }
}
