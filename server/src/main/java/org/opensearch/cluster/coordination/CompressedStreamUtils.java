/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.compress.Compressor;
import org.opensearch.common.compress.CompressorFactory;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.transport.BytesTransportRequest;

import java.io.IOException;

/**
 * A helper class to utilize the compressed stream.
 *
 * @opensearch.internal
 */
public final class CompressedStreamUtils {
    private static final Logger logger = LogManager.getLogger(CompressedStreamUtils.class);

    public static BytesReference createCompressedStream(Version version, CheckedConsumer<StreamOutput, IOException> outputConsumer)
        throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = new OutputStreamStreamOutput(CompressorFactory.defaultCompressor().threadLocalOutputStream(bStream))) {
            stream.setVersion(version);
            outputConsumer.accept(stream);
        }
        final BytesReference serializedByteRef = bStream.bytes();
        logger.trace("serialized writable object for node version [{}] with size [{}]", version, serializedByteRef.length());
        return serializedByteRef;
    }

    public static StreamInput decompressBytes(BytesTransportRequest request, NamedWriteableRegistry namedWriteableRegistry)
        throws IOException {
        final Compressor compressor = CompressorFactory.compressor(request.bytes());
        final StreamInput in;
        if (compressor != null) {
            in = new InputStreamStreamInput(compressor.threadLocalInputStream(request.bytes().streamInput()));
        } else {
            in = request.bytes().streamInput();
        }
        in.setVersion(request.version());
        return new NamedWriteableAwareStreamInput(in, namedWriteableRegistry);
    }
}
