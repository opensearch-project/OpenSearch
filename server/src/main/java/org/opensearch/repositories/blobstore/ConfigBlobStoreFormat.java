/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.blobstore;

import org.apache.lucene.store.IndexInput;
import org.opensearch.common.blobstore.AsyncMultiStreamBlobContainer;
import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.stream.write.WritePriority;
import org.opensearch.common.blobstore.transfer.RemoteTransferContainer;
import org.opensearch.common.blobstore.transfer.stream.OffsetRangeIndexInputStream;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.ToXContent;

import java.io.IOException;

/**
 * Format for writing short configurations to remote. Read interface does not exist as it not yet required. This format
 * should be used for writing data from in-memory to remote store where there is no need for checksum and the client
 * library for the remote store has inbuilt checksum capabilities while upload and download both. This format would
 * serialise the data in Json format and store it on remote store as is. This does not support compression yet (this
 * can be changed as required). In comparison to {@link ChecksumBlobStoreFormat}, this format does not add any additional
 * metadata (like header and footer) to the content. Hence, this format does not depend on {@code CodecUtil} from
 * Lucene library.
 *
 * @opensearch.internal
 */
public class ConfigBlobStoreFormat<T extends ToXContent> extends AbstractBlobStoreFormat<T> {

    /**
     * @param blobNameFormat format of the blobname in {@link String#format} format
     */
    public ConfigBlobStoreFormat(String blobNameFormat) {
        super(blobNameFormat, true);
    }

    public void writeAsyncWithUrgentPriority(T obj, BlobContainer blobContainer, String name, ActionListener<Void> listener)
        throws IOException {
        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
            write(obj, blobContainer, name, new NoneCompressor(), ToXContent.EMPTY_PARAMS, XContentType.JSON, null, null);
            listener.onResponse(null);
            return;
        }
        String blobName = blobName(name);
        BytesReference bytes = serialize(obj, blobName, new NoneCompressor(), ToXContent.EMPTY_PARAMS, XContentType.JSON, null, null);
        String resourceDescription = "BlobStoreFormat.writeAsyncWithPriority(blob=\"" + blobName + "\")";
        try (IndexInput input = new ByteArrayIndexInput(resourceDescription, BytesReference.toBytes(bytes))) {
            try (
                RemoteTransferContainer remoteTransferContainer = new RemoteTransferContainer(
                    blobName,
                    blobName,
                    bytes.length(),
                    true,
                    WritePriority.URGENT,
                    (size, position) -> new OffsetRangeIndexInputStream(input, size, position),
                    null,
                    false
                )
            ) {
                ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(remoteTransferContainer.createWriteContext(), listener);
            }
        }
    }
}
