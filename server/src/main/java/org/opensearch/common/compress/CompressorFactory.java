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

package org.opensearch.common.compress;

import org.opensearch.common.Nullable;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.compress.NotXContentException;
import org.opensearch.core.xcontent.MediaTypeRegistry;

import java.io.IOException;
import java.util.Objects;

/**
 * Factory to create a compressor instance.
 *
 * @opensearch.internal
 */
public class CompressorFactory {

    public static final Compressor DEFLATE_COMPRESSOR = new DeflateCompressor();

    public static final Compressor ZSTD_COMPRESSOR = new ZstdCompressor();

    public static final Compressor NONE_COMPRESSOR = new NoneCompressor();

    public static boolean isCompressed(BytesReference bytes) {
        return compressor(bytes) != null;
    }

    public static Compressor defaultCompressor() {
        return DEFLATE_COMPRESSOR;
    }

    @Nullable
    public static Compressor compressor(BytesReference bytes) {
        if (DEFLATE_COMPRESSOR.isCompressed(bytes)) {
            // bytes should be either detected as compressed or as xcontent,
            // if we have bytes that can be either detected as compressed or
            // as a xcontent, we have a problem
            assert MediaTypeRegistry.xContentType(bytes) == null;
            return DEFLATE_COMPRESSOR;
        } else if (ZSTD_COMPRESSOR.isCompressed(bytes)) {
            assert MediaTypeRegistry.xContentType(bytes) == null;
            return ZSTD_COMPRESSOR;
        }

        if (MediaTypeRegistry.xContentType(bytes) == null) {
            throw new NotXContentException("Compressor detection can only be called on some xcontent bytes or compressed xcontent bytes");
        }

        return null;
    }

    /**
     * Uncompress the provided data, data can be detected as compressed using {@link #isCompressed(BytesReference)}.
     */
    public static BytesReference uncompressIfNeeded(BytesReference bytes) throws IOException {
        Compressor compressor = compressor(Objects.requireNonNull(bytes, "the BytesReference must not be null"));
        return compressor == null ? bytes : compressor.uncompress(bytes);
    }

    /** Decompress the provided {@link BytesReference}. */
    public static BytesReference uncompress(BytesReference bytes) throws IOException {
        Compressor compressor = compressor(bytes);
        if (compressor == null) {
            throw new NotCompressedException();
        }
        return compressor.uncompress(bytes);
    }
}
