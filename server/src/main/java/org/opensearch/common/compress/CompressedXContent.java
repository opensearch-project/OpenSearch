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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.io.Streams;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.metadata.compress.CompressedData;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32;
import java.util.zip.CheckedOutputStream;

/**
 * Similar class to the {@link String} class except that it internally stores
 * data using a compressed representation in order to require less permanent
 * memory. Note that the compressed string might still sometimes need to be
 * decompressed in order to perform equality checks or to compute hash codes.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class CompressedXContent {

    private static int crc32(BytesReference data) {
        CRC32 crc32 = new CRC32();
        try {
            data.writeTo(new CheckedOutputStream(Streams.NULL_OUTPUT_STREAM, crc32));
        } catch (IOException bogus) {
            // cannot happen
            throw new Error(bogus);
        }
        return (int) crc32.getValue();
    }

    private final CompressedData compressedData;

    /**
     * Create a {@link CompressedXContent} from a {@link CompressedData} instance.
     */
    // Used for serialization
    public CompressedXContent(CompressedData compressedData) {
        this.compressedData = compressedData;
        assertConsistent();
    }

    /**
     * Create a {@link CompressedXContent} out of a {@link ToXContent} instance.
     */
    public CompressedXContent(ToXContent xcontent, ToXContent.Params params) throws IOException {
        BytesStreamOutput bStream = new BytesStreamOutput();
        OutputStream compressedStream = CompressorRegistry.defaultCompressor().threadLocalOutputStream(bStream);
        CRC32 crc32 = new CRC32();
        OutputStream checkedStream = new CheckedOutputStream(compressedStream, crc32);
        try (XContentBuilder builder = XContentFactory.jsonBuilder(checkedStream)) {
            if (xcontent.isFragment()) {
                builder.startObject();
            }
            xcontent.toXContent(builder, params);
            if (xcontent.isFragment()) {
                builder.endObject();
            }
        }
        this.compressedData = new CompressedData(BytesReference.toBytes(bStream.bytes()), (int) crc32.getValue());
        assertConsistent();
    }

    /**
     * Create a {@link CompressedXContent} out of a serialized {@link ToXContent}
     * that may already be compressed.
     */
    public CompressedXContent(BytesReference data) throws IOException {
        Compressor compressor = CompressorRegistry.compressor(data);
        if (compressor != null) {
            // already compressed...
            byte[] bytes = BytesReference.toBytes(data);
            this.compressedData = new CompressedData(bytes, crc32(uncompress(bytes)));
        } else {
            this.compressedData = new CompressedData(
                BytesReference.toBytes(CompressorRegistry.defaultCompressor().compress(data)),
                crc32(data)
            );
        }
        assertConsistent();
    }

    private void assertConsistent() {
        assert CompressorRegistry.compressor(new BytesArray(compressedData.compressedBytes())) != null;
        assert compressedData.checksum() == crc32(uncompressed());
    }

    public CompressedXContent(byte[] data) throws IOException {
        this(new BytesArray(data));
    }

    public CompressedXContent(String str) throws IOException {
        this(new BytesArray(str.getBytes(StandardCharsets.UTF_8)));
    }

    /** Return the compressed bytes. */
    public byte[] compressed() {
        return compressedData.compressedBytes();
    }

    /** Return the compressed bytes as a {@link BytesReference}. */
    public BytesReference compressedReference() {
        return new BytesArray(compressedData.compressedBytes());
    }

    /** Return the uncompressed bytes. */
    public BytesReference uncompressed() {
        return uncompress(compressedData.compressedBytes());
    }

    private static BytesReference uncompress(byte[] bytes) {
        try {
            return CompressorRegistry.uncompress(new BytesArray(bytes));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot decompress compressed string", e);
        }
    }

    public String string() {
        return uncompressed().utf8ToString();
    }

    /**
     * Returns the underlying {@link CompressedData}.
     *
     * @return the compressed data
     */
    public CompressedData compressedData() {
        return compressedData;
    }

    public static CompressedXContent readCompressedString(StreamInput in) throws IOException {
        CompressedData data = new CompressedData(in);
        return new CompressedXContent(data);
    }

    public void writeTo(StreamOutput out) throws IOException {
        compressedData.writeTo(out);
    }

    public void writeVerifiableTo(BufferedChecksumStreamOutput out) throws IOException {
        out.writeInt(compressedData.checksum());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompressedXContent that = (CompressedXContent) o;
        return compressedData.equals(that.compressedData);
    }

    @Override
    public int hashCode() {
        return compressedData.hashCode();
    }

    @Override
    public String toString() {
        return string();
    }
}
