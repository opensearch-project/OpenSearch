/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.opensearch.index.codec.PerFieldMappingPostingFormatCodec;
import org.opensearch.index.mapper.MapperService;

import java.util.Collections;
import java.util.Set;

/**
 *
 * Extends {@link FilterCodec} to reuse the functionality of Lucene Codec.
 * Supports two modes zstd and zstd_no_dict.
 *
 * @opensearch.internal
 */
public abstract class Lucene95CustomCodec extends FilterCodec {

    /** Default compression level used for compression */
    public static final int DEFAULT_COMPRESSION_LEVEL = 3;

    /** Each mode represents a compression algorithm. */
    public enum Mode {
        /**
         * ZStandard mode with dictionary
         */
        ZSTD("ZSTD", Set.of("zstd")),
        /**
         * ZStandard mode without dictionary
         */
        ZSTD_NO_DICT("ZSTDNODICT", Set.of("zstd_no_dict")),
        /**
         * Deprecated ZStandard mode, added for backward compatibility to support indices created in 2.9.0 where
         * both ZSTD and ZSTD_NO_DICT used Lucene95CustomCodec underneath. This should not be used to
         * create new indices.
         */
        ZSTD_DEPRECATED("Lucene95CustomCodec", Collections.emptySet());

        private final String codec;
        private final Set<String> aliases;

        Mode(String codec, Set<String> aliases) {
            this.codec = codec;
            this.aliases = aliases;
        }

        /**
         * Returns the Codec that is registered with Lucene
         */
        public String getCodec() {
            return codec;
        }

        /**
         * Returns the aliases of the Codec
         */
        public Set<String> getAliases() {
            return aliases;
        }
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
     * @param compressionLevel The compression level.
     */
    public Lucene95CustomCodec(Mode mode, int compressionLevel) {
        super(mode.getCodec(), new Lucene95Codec());
        this.storedFieldsFormat = new Lucene95CustomStoredFieldsFormat(mode, compressionLevel);
    }

    /**
     * Creates a new compression codec with the given compression level. We use
     * lowercase letters when registering the codec so that we remain consistent with
     * the other compression codecs: default, lucene_default, and best_compression.
     *
     * @param mode The compression codec (ZSTD or ZSTDNODICT).
     * @param compressionLevel The compression level.
     * @param mapperService The mapper service.
     * @param logger The logger.
     */
    public Lucene95CustomCodec(Mode mode, int compressionLevel, MapperService mapperService, Logger logger) {
        super(mode.getCodec(), new PerFieldMappingPostingFormatCodec(Lucene95Codec.Mode.BEST_SPEED, mapperService, logger));
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
