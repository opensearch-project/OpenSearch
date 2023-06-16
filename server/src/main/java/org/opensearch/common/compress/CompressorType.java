/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.compress;

/**
 * Supported compression types
 *
 * @opensearch.internal
 */
public enum CompressorType {

    DEFLATE {
        @Override
        public Compressor compressor() {
            return CompressorFactory.DEFLATE_COMPRESSOR;
        }
    },

    ZSTD {
        @Override
        public Compressor compressor() {
            return CompressorFactory.ZSTD_COMPRESSOR;
        }
    },

    NONE {
        @Override
        public Compressor compressor() {
            return CompressorFactory.NONE_COMPRESSOR;
        }
    };

    public abstract Compressor compressor();
}
