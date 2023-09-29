package org.opensearch.http.netty4;

import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.HttpContentDecompressor;

import static org.opensearch.http.netty4.Netty4HttpServerTransport.SHOULD_DECOMPRESS;

public class Netty4ConditionalDecompressor extends HttpContentDecompressor {
    @Override
    protected EmbeddedChannel newContentDecoder(String contentEncoding) throws Exception {
        if (Boolean.FALSE.equals(ctx.channel().attr(SHOULD_DECOMPRESS).get())) {
            return super.newContentDecoder("identity");
        }
        return super.newContentDecoder(contentEncoding);
    }
}
