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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.repositories.blobstore;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.BytesRef;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.io.Streams;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.gateway.CorruptStateException;
import org.opensearch.index.store.exception.ChecksumCombinationException;
import org.opensearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.common.blobstore.transfer.RemoteTransferContainer.checksumOfChecksum;

/**
 * Snapshot metadata file format used in v2.0 and above
 *
 * @opensearch.internal
 */
public final class ChecksumBlobStoreFormat<T extends ToXContent> extends AbstractBlobStoreFormat<T> {

    // Serialization parameters to specify correct context for metadata serialization
    public static final ToXContent.Params SNAPSHOT_ONLY_FORMAT_PARAMS;

    static {
        Map<String, String> snapshotOnlyParams = new HashMap<>();
        // when metadata is serialized certain elements of the metadata shouldn't be included into snapshot
        // exclusion of these elements is done by setting Metadata.CONTEXT_MODE_PARAM to Metadata.CONTEXT_MODE_SNAPSHOT
        snapshotOnlyParams.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_SNAPSHOT);
        // serialize SnapshotInfo using the SNAPSHOT mode
        snapshotOnlyParams.put(SnapshotInfo.CONTEXT_MODE_PARAM, SnapshotInfo.CONTEXT_MODE_SNAPSHOT);
        SNAPSHOT_ONLY_FORMAT_PARAMS = new ToXContent.MapParams(snapshotOnlyParams);
    }

    // The format version
    public static final int VERSION = 1;

    private final String codec;

    private final CheckedFunction<XContentParser, T, IOException> reader;

    /**
     * @param codec          codec name
     * @param blobNameFormat format of the blobname in {@link String#format} format
     * @param reader         prototype object that can deserialize T from XContent
     */
    public ChecksumBlobStoreFormat(String codec, String blobNameFormat, CheckedFunction<XContentParser, T, IOException> reader) {
        super(blobNameFormat, false);
        this.reader = reader;
        this.codec = codec;
    }

    /**
     * Reads and parses the blob with given name, applying name translation using the {link #blobName} method
     *
     * @param blobContainer blob container
     * @param name          name to be translated into
     * @return parsed blob object
     */
    public T read(BlobContainer blobContainer, String name, NamedXContentRegistry namedXContentRegistry) throws IOException {
        String blobName = blobName(name);
        return deserialize(blobName, namedXContentRegistry, Streams.readFully(blobContainer.readBlob(blobName)));
    }

    public String blobName(String name) {
        return String.format(Locale.ROOT, getBlobNameFormat(), name);
    }

    public T deserialize(String blobName, NamedXContentRegistry namedXContentRegistry, BytesReference bytes) throws IOException {
        final String resourceDesc = "ChecksumBlobStoreFormat.readBlob(blob=\"" + blobName + "\")";
        try {
            final IndexInput indexInput = bytes.length() > 0
                ? new ByteBuffersIndexInput(new ByteBuffersDataInput(Arrays.asList(BytesReference.toByteBuffers(bytes))), resourceDesc)
                : new ByteArrayIndexInput(resourceDesc, BytesRef.EMPTY_BYTES);
            CodecUtil.checksumEntireFile(indexInput);
            CodecUtil.checkHeader(indexInput, codec, VERSION, VERSION);
            long filePointer = indexInput.getFilePointer();
            long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;
            try (
                XContentParser parser = XContentHelper.createParser(
                    namedXContentRegistry,
                    LoggingDeprecationHandler.INSTANCE,
                    bytes.slice((int) filePointer, (int) contentSize),
                    XContentType.SMILE
                )
            ) {
                return reader.apply(parser);
            }
        } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
            // we trick this into a dedicated exception with the original stacktrace
            throw new CorruptStateException(ex);
        }
    }

    /**
     * Writes blob with resolving the blob name using {@link #blobName} method.
     * <p>
     * The blob will optionally by compressed.
     *
     * @param obj                 object to be serialized
     * @param blobContainer       blob container
     * @param name                blob name
     * @param compressor          whether to use compression
     */
    public void write(final T obj, final BlobContainer blobContainer, final String name, final Compressor compressor) throws IOException {
        write(obj, blobContainer, name, compressor, SNAPSHOT_ONLY_FORMAT_PARAMS, XContentType.SMILE, codec, VERSION);
    }

    /**
     * Internally calls {@link #writeAsyncWithPriority} with {@link WritePriority#NORMAL}
     */
    public void writeAsync(
        final T obj,
        final BlobContainer blobContainer,
        final String name,
        final Compressor compressor,
        ActionListener<Void> listener,
        final ToXContent.Params params
    ) throws IOException {
        // use NORMAL priority by default
        this.writeAsyncWithPriority(obj, blobContainer, name, compressor, WritePriority.NORMAL, listener, params);
    }

    /**
     * Internally calls {@link #writeAsyncWithPriority} with {@link WritePriority#URGENT}
     * <p>
     * <b>NOTE:</b> We use this method to upload urgent priority objects like cluster state to remote stores.
     * Use {@link #writeAsync(ToXContent, BlobContainer, String, Compressor, ActionListener, ToXContent.Params)} for
     * other use cases.
     */
    public void writeAsyncWithUrgentPriority(
        final T obj,
        final BlobContainer blobContainer,
        final String name,
        final Compressor compressor,
        ActionListener<Void> listener,
        final ToXContent.Params params
    ) throws IOException {
        this.writeAsyncWithPriority(obj, blobContainer, name, compressor, WritePriority.URGENT, listener, params);
    }

    /**
     * Method to writes blob with resolving the blob name using {@link #blobName} method with specified
     * {@link WritePriority}. Leverages the multipart upload if supported by the blobContainer.
     *
     * @param obj                 object to be serialized
     * @param blobContainer       blob container
     * @param name                blob name
     * @param compressor          whether to use compression
     * @param priority            write priority to be used
     * @param listener            listener to listen to write result
     * @param params              ToXContent params
     */
    private void writeAsyncWithPriority(
        final T obj,
        final BlobContainer blobContainer,
        final String name,
        final Compressor compressor,
        final WritePriority priority,
        ActionListener<Void> listener,
        final ToXContent.Params params
    ) throws IOException {
        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
            write(obj, blobContainer, name, compressor, params, XContentType.SMILE, codec, VERSION);
            listener.onResponse(null);
            return;
        }
        final String blobName = blobName(name);
        final BytesReference bytes = serialize(obj, blobName, compressor, params);
        final String resourceDescription = "ChecksumBlobStoreFormat.writeAsyncWithPriority(blob=\"" + blobName + "\")";
        try (IndexInput input = new ByteArrayIndexInput(resourceDescription, BytesReference.toBytes(bytes))) {
            long expectedChecksum;
            try {
                expectedChecksum = checksumOfChecksum(input.clone(), 8);
            } catch (Exception e) {
                throw new ChecksumCombinationException(
                    "Potentially corrupted file: Checksum combination failed while combining stored checksum "
                        + "and calculated checksum of stored checksum",
                    resourceDescription,
                    e
                );
            }

            try (
                RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                    blobName,
                    blobName,
                    bytes.length(),
                    true,
                    priority,
                    (size, position) -> new OffsetRangeIndexInputStream(input, size, position),
                    expectedChecksum,
                    ((AsyncMultiStreamBlobContainer) blobContainer).remoteIntegrityCheckSupported()
                )
            ) {
                ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(remoteTransferContainer.createWriteContext(), listener);
            }
        }
    }

    public BytesReference serialize(final T obj, final String blobName, final Compressor compressor, final ToXContent.Params params)
        throws IOException {
        return serialize(obj, blobName, compressor, params, XContentType.SMILE, codec, VERSION);
    }
}
