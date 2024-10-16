/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.lucene.store.IndexOutputOutputStream;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.io.stream.Writeable.Writer;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.gateway.CorruptStateException;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * Checksum File format used to serialize/deserialize {@link Writeable} objects
 *
 * @opensearch.internal
 */
public class ChecksumWritableBlobStoreFormat<T extends Writeable> {

    public static final int VERSION = 1;

    private static final int BUFFER_SIZE = 4096;

    private final String codec;
    private final CheckedFunction<StreamInput, T, IOException> reader;

    public ChecksumWritableBlobStoreFormat(String codec, CheckedFunction<StreamInput, T, IOException> reader) {
        this.codec = codec;
        this.reader = reader;
    }

    public BytesReference serialize(final T obj, final String blobName, final Compressor compressor) throws IOException {
        return serialize((out, unSerializedObj) -> unSerializedObj.writeTo(out), obj, blobName, compressor);
    }

    public BytesReference serialize(final Writer<T> writer, T obj, final String blobName, final Compressor compressor) throws IOException {
        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            try (
                OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "ChecksumBlobStoreFormat.writeBlob(blob=\"" + blobName + "\")",
                    blobName,
                    outputStream,
                    BUFFER_SIZE
                )
            ) {
                CodecUtil.writeHeader(indexOutput, codec, VERSION);

                try (OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput) {
                    @Override
                    public void close() throws IOException {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    }
                }; StreamOutput stream = new OutputStreamStreamOutput(compressor.threadLocalOutputStream(indexOutputOutputStream));) {
                    // TODO The stream version should be configurable
                    stream.setVersion(Version.CURRENT);
                    writer.write(stream, obj);
                }
                CodecUtil.writeFooter(indexOutput);
            }
            return outputStream.bytes();
        }
    }

    public T deserialize(String blobName, BytesReference bytes) throws IOException {
        final String resourceDesc = "ChecksumBlobStoreFormat.readBlob(blob=\"" + blobName + "\")";
        try {
            final IndexInput indexInput = bytes.length() > 0
                ? new ByteBuffersIndexInput(new ByteBuffersDataInput(Arrays.asList(BytesReference.toByteBuffers(bytes))), resourceDesc)
                : new ByteArrayIndexInput(resourceDesc, BytesRef.EMPTY_BYTES);
            CodecUtil.checksumEntireFile(indexInput);
            CodecUtil.checkHeader(indexInput, codec, VERSION, VERSION);
            long filePointer = indexInput.getFilePointer();
            long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;
            BytesReference bytesReference = bytes.slice((int) filePointer, (int) contentSize);
            Compressor compressor = CompressorRegistry.compressorForWritable(bytesReference);
            try (StreamInput in = new InputStreamStreamInput(compressor.threadLocalInputStream(bytesReference.streamInput()))) {
                return reader.apply(in);
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we trick this into a dedicated exception with the original stacktrace
            throw new CorruptStateException(ex);
        }
    }

}
