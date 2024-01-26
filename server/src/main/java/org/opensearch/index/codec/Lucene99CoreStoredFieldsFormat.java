/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.lucene90.DeflateWithPresetDictCompressionMode;
import org.apache.lucene.codecs.lucene90.LZ4WithPresetDictCompressionMode;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;

import java.io.IOException;
import java.util.Objects;

/**
 * Stored field format used by pluggable codec
 */
public class Lucene99CoreStoredFieldsFormat extends StoredFieldsFormat {

    /**
     * A key that we use to map to a mode
     */
    public static final String MODE_KEY = Lucene99CoreStoredFieldsFormat.class.getSimpleName() + ".mode";

    private final Lucene99Codec.Mode mode;

    /**
     * default constructor
     */
    public Lucene99CoreStoredFieldsFormat() {
        this(Lucene99Codec.Mode.BEST_SPEED);
    }

    /**
     * Creates a new instance.
     *
     * @param mode The mode represents ZSTD or ZSTDNODICT
     */

    public Lucene99CoreStoredFieldsFormat(Lucene99Codec.Mode mode) {
        this.mode = Objects.requireNonNull(mode);
    }

    /**
     * Returns a {@link StoredFieldsReader} to load stored fields.
     *
     * @param directory The index directory.
     * @param si        The SegmentInfo that stores segment information.
     * @param fn        The fieldInfos.
     * @param context   The IOContext that holds additional details on the merge/search context.
     */
    @Override
    public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context) throws IOException {
        if (si.getAttribute(MODE_KEY) != null) {
            String value = si.getAttribute(MODE_KEY);
            Lucene99Codec.Mode mode = Lucene99Codec.Mode.valueOf(value);
            return impl(mode).fieldsReader(directory, si, fn, context);
        } else {
            throw new IllegalStateException("missing value for " + MODE_KEY + " for segment: " + si.name);
        }

    }

    /**
     * Returns a {@link StoredFieldsReader} to write stored fields.
     *
     * @param directory The index directory.
     * @param si        The SegmentInfo that stores segment information.
     * @param context   The IOContext that holds additional details on the merge/search context.
     */

    @Override
    public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
        String previous = si.putAttribute(MODE_KEY, mode.name());
        if (previous != null && previous.equals(mode.name()) == false) {
            throw new IllegalStateException(
                "found existing value for " + MODE_KEY + " for segment: " + si.name + "old=" + previous + ", new=" + mode.name()
            );
        }
        return impl(mode).fieldsWriter(directory, si, context);
    }

    StoredFieldsFormat impl(Lucene99Codec.Mode mode) {
        switch (mode) {
            case BEST_SPEED:
                return getLZ4CompressingStoredFieldsFormat();
            case BEST_COMPRESSION:
                return getZlibCompressingStoredFieldsFormat();
            default:
                throw new AssertionError();
        }
    }

    public Lucene99Codec.Mode getMode() {
        return mode;
    }

    // Shoot for 10 sub blocks of 48kB each.
    private static final int BEST_COMPRESSION_BLOCK_LENGTH = 10 * 48 * 1024;

    /**
     * Compression mode for {@link Lucene90StoredFieldsFormat.Mode#BEST_COMPRESSION}
     */
    public static final CompressionMode BEST_COMPRESSION_MODE = new DeflateWithPresetDictCompressionMode();

    // Shoot for 10 sub blocks of 8kB each.
    private static final int BEST_SPEED_BLOCK_LENGTH = 10 * 16 * 1024;

    /**
     * Compression mode for {@link Lucene90StoredFieldsFormat.Mode#BEST_SPEED}
     */
    public static final CompressionMode BEST_SPEED_MODE = new LZ4WithPresetDictCompressionMode();

    private StoredFieldsFormat getLZ4CompressingStoredFieldsFormat() {
        return new Lucene90CompressingStoredFieldsFormat(
            "Lucene90StoredFieldsFastData",
            BEST_SPEED_MODE,
            BEST_SPEED_BLOCK_LENGTH,
            1024,
            10
        );
    }

    private StoredFieldsFormat getZlibCompressingStoredFieldsFormat() {
        return new Lucene90CompressingStoredFieldsFormat(
            "Lucene90StoredFieldsHighData",
            BEST_COMPRESSION_MODE,
            BEST_COMPRESSION_BLOCK_LENGTH,
            4096,
            10
        );
    }

}
