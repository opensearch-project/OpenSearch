/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

/** Custom codec for different compression algorithm */
public class ZstdCodec extends Lucene92CustomCodec {

    /** new codec for a given compression algorithm and compression level */
    public ZstdCodec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    public ZstdCodec(int compressionLevel) {
        super(Mode.ZSTD, compressionLevel);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
