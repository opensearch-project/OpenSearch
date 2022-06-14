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

    public static final String MODE_KEY = Lucene90CustomStoredFieldsFormat.class.getSimpleName() + ".mode";

    final Lucene92CustomCodec.Mode mode;

    private int compressionLevel;

    /** default constructor */
    public Lucene90CustomStoredFieldsFormat() {
        this(Lucene92CustomCodec.Mode.LZ4_NATIVE, Lucene92CustomCodec.defaultCompressionLevel);
    }

    /** Stored fields format with specified compression algo. */
    public Lucene90CustomStoredFieldsFormat(Lucene92CustomCodec.Mode mode, int compressionLevel) {
        this.mode = Objects.requireNonNull(mode);
        this.compressionLevel = compressionLevel;
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
                "found existing value for " + MODE_KEY + " for segment: " + si.name + "old=" + previous + ", new=" + mode.name()
            );
        }
        return impl(mode).fieldsWriter(directory, si, context);
    }

    StoredFieldsFormat impl(Lucene92CustomCodec.Mode mode) {
        switch (mode) {
            case ZSTD:
                return new Lucene90CompressingStoredFieldsFormat("CustomStoredFieldsZstd", ZSTD_MODE, ZSTD_BLOCK_LENGTH, 4096, 10);

            case ZSTD_NO_DICT:
                return new Lucene90CompressingStoredFieldsFormat(
                    "CustomStoredFieldsZstdNoDict",
                    ZSTD_MODE_NO_DICT,
                    ZSTD_BLOCK_LENGTH,
                    4096,
                    10
                );

            case LZ4_NATIVE:
                return new Lucene90CompressingStoredFieldsFormat("CustomStoredFieldsLz4", LZ4_MODE, LZ4_NATIVE_BLOCK_LENGTH, 1024, 10);

            default:
                throw new AssertionError();
        }
    }

    public final CompressionMode ZSTD_MODE = new ZstdCompressionMode(compressionLevel);
    public final CompressionMode ZSTD_MODE_NO_DICT = new ZstdNoDictCompressionMode(compressionLevel);
    public final CompressionMode LZ4_MODE = new LZ4CompressionMode();
}
