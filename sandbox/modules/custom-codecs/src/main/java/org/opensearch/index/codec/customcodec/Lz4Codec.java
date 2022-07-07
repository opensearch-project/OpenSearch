/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

/**
 * Lz4Codec provides a native LZ4 implementation based on the <a href="https://github.com/lz4/lz4-java">lz4-java</a> library.
 */
public class Lz4Codec extends Lucene92CustomCodec {

    /**
     * Creates a new Lz4Codec instance.
     */
    public Lz4Codec() {
        super(Mode.LZ4);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
}
