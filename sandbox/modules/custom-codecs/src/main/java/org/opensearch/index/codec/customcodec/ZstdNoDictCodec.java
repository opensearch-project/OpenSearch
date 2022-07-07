/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

/** Custom codec for different compression algorithm */
public class ZstdNoDictCodec extends Lucene92CustomCodec {

    public ZstdNoDictCodec() {
        this(DEFAULT_COMPRESSION_LEVEL);
    }

    public ZstdNoDictCodec(int compressionLevel) {
        super(Mode.ZSTDNODICT, compressionLevel);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
