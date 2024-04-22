/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.store.IndexOutputOutputStream;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Locale;
import java.util.Objects;

/**
 * Provides common methods, variables that can be used by the implementors.
 *
 * @opensearch.internal
 */
public class AbstractBlobStoreFormat<T extends ToXContent> {

    private static final int BUFFER_SIZE = 4096;

    private final String blobNameFormat;

    private final boolean skipHeaderFooter;

    /**
     * @param blobNameFormat format of the blobname in {@link String#format} format
     */
    public AbstractBlobStoreFormat(String blobNameFormat, boolean skipHeaderFooter) {
        this.blobNameFormat = blobNameFormat;
        this.skipHeaderFooter = skipHeaderFooter;
    }

    protected String blobName(String name) {
        return String.format(Locale.ROOT, blobNameFormat, name);
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will optionally by compressed.
     *
     * @param obj           object to be serialized
     * @param blobContainer blob container
     * @param name          blob name
     * @param compressor    whether to use compression
     * @param params        ToXContent params
     * @param codec         codec used
     * @param version       version used
     */
    protected void write(
        final T obj,
        final BlobContainer blobContainer,
        final String name,
        final Compressor compressor,
        final ToXContent.Params params,
        XContentType xContentType,
        String codec,
        Integer version
    ) throws IOException {
        final String blobName = blobName(name);
        final BytesReference bytes = serialize(obj, blobName, compressor, params, xContentType, codec, version);
        blobContainer.writeBlob(blobName, bytes.streamInput(), bytes.length(), false);
    }

    public BytesReference serialize(
        final T obj,
        final String blobName,
        final Compressor compressor,
        final ToXContent.Params params,
        XContentType xContentType,
        String codec,
        Integer version
    ) throws IOException {
        assert skipHeaderFooter || (Objects.nonNull(codec) && Objects.nonNull(version));
        try (BytesStreamOutput outputStream = new BytesStreamOutput()) {
            try (
                OutputStreamIndexOutput indexOutput = new OutputStreamIndexOutput(
                    "ChecksumBlobStoreFormat.writeBlob(blob=\"" + blobName + "\")",
                    blobName,
                    outputStream,
                    BUFFER_SIZE
                )
            ) {
                if (skipHeaderFooter == false) {
                    CodecUtil.writeHeader(indexOutput, codec, version);
                }
                try (OutputStream indexOutputOutputStream = new IndexOutputOutputStream(indexOutput) {
                    @Override
                    public void close() {
                        // this is important since some of the XContentBuilders write bytes on close.
                        // in order to write the footer we need to prevent closing the actual index input.
                    }
                };
                    XContentBuilder builder = MediaTypeRegistry.contentBuilder(
                        xContentType,
                        compressor.threadLocalOutputStream(indexOutputOutputStream)
                    )
                ) {
                    builder.startObject();
                    obj.toXContent(builder, params);
                    builder.endObject();
                }
                if (skipHeaderFooter == false) {
                    CodecUtil.writeFooter(indexOutput);
                }
            }
            return outputStream.bytes();
        }
    }

    protected String getBlobNameFormat() {
        return blobNameFormat;
    }
}
