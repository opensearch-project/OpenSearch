/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpHeaderValues;

/**
 * A {@link HttpContentDecompressor} that does not support the {@code zstd} content encoding with the
 * zstd-jni version bundled with OpenSearch. Such content is passed through unmodified, as for any other
 * unsupported content encoding.
 * This mirrors the ZSTD exclusion already applied to the outbound {@code encoder_compress} handler.
 */
class Netty4HttpContentDecompressor extends HttpContentDecompressor {
    @Override
    protected EmbeddedChannel newContentDecoder(String contentEncoding) throws Exception {
        if (HttpHeaderValues.ZSTD.contentEqualsIgnoreCase(contentEncoding)) {
            // not supported: pass the content through unmodified, as for any other unknown encoding
            return null;
        }
        return super.newContentDecoder(contentEncoding);
    }
}
