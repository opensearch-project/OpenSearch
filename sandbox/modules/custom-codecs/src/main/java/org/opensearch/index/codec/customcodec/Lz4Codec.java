/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

/** Custom codec for different compression algorithm */
public class Lz4Codec extends Lucene92CustomCodec {

    public Lz4Codec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    public Lz4Codec(int compressionLevel) {
        super(Mode.LZ4, compressionLevel);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
