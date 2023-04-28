package org.opensearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.compress.CompressorFactory;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A helper class to utilize the compressed stream.
 */
public class CompressionHelper {
    private static final Logger logger = LogManager.getLogger(CompressionHelper.class);

    public static BytesReference serializedWrite(Writeable writer, Version nodeVersion, boolean streamBooleanFlag) throws IOException {
        final BytesStreamOutput bStream = new BytesStreamOutput();
        try (StreamOutput stream = new OutputStreamStreamOutput(CompressorFactory.COMPRESSOR.threadLocalOutputStream(bStream))) {
            stream.setVersion(nodeVersion);
            stream.writeBoolean(streamBooleanFlag);
            writer.writeTo(stream);
        }
        final BytesReference serializedByteRef = bStream.bytes();
        logger.trace(
                "serialized writable object for node version [{}] with size [{}]",
                nodeVersion,
                serializedByteRef.length()
        );
        return serializedByteRef;
    }
}
