/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;

import com.intel.qat.QatZipper;

import static com.intel.qat.QatZipper.Algorithm;
import static com.intel.qat.QatZipper.PollingMode;

abstract class Lucene99QatCodec extends FilterCodec {

    public static QatZipper getCompressor(Algorithm algorithm, int level, QatZipper.Mode mode, PollingMode pmode) {
        return new QatZipper(algorithm, level, mode, pmode);
    }

    public static QatZipper getCompressor(Algorithm algorithm, QatZipper.Mode mode, PollingMode pmode) {
        return new QatZipper(algorithm, mode, pmode);
    }

    public static final int DEFAULT_COMPRESSION_LEVEL = 6;

    /** Each mode represents a compression algorithm. */
    public enum Mode {
        QDEFLATE,
        QLZ4
    }

    public static final Setting<String> INDEX_CODEC_MODE_SETTING = new Setting<>("index.codec.mode", "hardware", s -> {
        switch (s) {
            case "auto":
            case "hardware":
                return s;
            default:
                throw new IllegalArgumentException("unknown value for [index.codec.mode] must be one of [auto, hardware] but was: " + s);
        }
    }, Property.IndexScope, Property.NodeScope);

    private final StoredFieldsFormat storedFieldsFormat;

    /**
     * new codec for a given compression algorithm and default compression level
     */
    public Lucene99QatCodec(Mode mode, String accelerationMode) {
        this(mode, DEFAULT_COMPRESSION_LEVEL, accelerationMode);
    }

    public Lucene99QatCodec(Mode mode, int compressionLevel, String accelerationMode) {
        super(mode.name(), new Lucene99Codec());
        this.storedFieldsFormat = new Lucene99QatStoredFieldsFormat(mode, compressionLevel, accelerationMode);
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
