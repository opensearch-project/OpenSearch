/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.apache.lucene.codecs.experimental;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene92.Lucene92Codec;

/** Custom codec for different compression algorithm */
public class Lucene92CustomCodec extends FilterCodec {

    public static final int defaultCompressionLevel = 6;
    private final StoredFieldsFormat storedFieldsFormat;
    private int compressionLevel;

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
        this(Mode.LZ4_NATIVE, defaultCompressionLevel);
    }

    /** new codec for a given compression algorithm and default compression level */
    public Lucene92CustomCodec(Mode compressionMode) {
        this(compressionMode, defaultCompressionLevel);
    }

    /** new codec for a given compression algorithm and compression level */
    public Lucene92CustomCodec(Mode compressionMode, int compressionLevel) {
        super("Lucene92CustomCodec", new Lucene92Codec());
        this.compressionLevel = compressionLevel;

        switch (compressionMode) {
            case ZSTD:
                if (this.compressionLevel < 1 || this.compressionLevel > 22) throw new IllegalArgumentException(
                    "Invalid compression level"
                );

                this.storedFieldsFormat = new Lucene90CustomStoredFieldsFormat(Mode.ZSTD, compressionLevel);
                break;
            case ZSTD_NO_DICT:
                if (this.compressionLevel < 1 || this.compressionLevel > 22) throw new IllegalArgumentException(
                    "Invalid compression level"
                );

                this.storedFieldsFormat = new Lucene90CustomStoredFieldsFormat(Mode.ZSTD_NO_DICT, compressionLevel);
                break;
            case LZ4_NATIVE:
                this.storedFieldsFormat = new Lucene90CustomStoredFieldsFormat(Mode.LZ4_NATIVE, compressionLevel);
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
