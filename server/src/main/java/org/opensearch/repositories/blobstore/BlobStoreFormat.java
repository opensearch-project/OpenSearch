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
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Standard format that has only content for writes. Read interface does not exist as it not yet required.
 *
 * @opensearch.internal
 */
public class BlobStoreFormat<T extends ToXContent> {

    private final String blobNameFormat;

    /**
     * @param blobNameFormat format of the blobname in {@link String#format} format
     */
    public BlobStoreFormat(String blobNameFormat) {
        this.blobNameFormat = blobNameFormat;
    }

    private String blobName(String name) {
        return String.format(Locale.ROOT, blobNameFormat, name);
    }

    private BytesReference serialize(final T obj) throws IOException {
        try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()) {
            xContentBuilder.startObject();
            obj.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            xContentBuilder.endObject();
            return BytesReference.bytes(xContentBuilder);
        }
    }

    public void writeAsyncWithUrgentPriority(T obj, BlobContainer blobContainer, String name, ActionListener<Void> listener)
        throws IOException {
        if (blobContainer instanceof AsyncMultiStreamBlobContainer == false) {
            write(obj, blobContainer, name);
            listener.onResponse(null);
            return;
        }
        String blobName = blobName(name);
        BytesReference bytes = serialize(obj);
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
                    ((AsyncMultiStreamBlobContainer) blobContainer).remoteIntegrityCheckSupported()
                )
            ) {
                ((AsyncMultiStreamBlobContainer) blobContainer).asyncBlobUpload(remoteTransferContainer.createWriteContext(), listener);
            }
        }
    }

    private void write(final T obj, final BlobContainer blobContainer, final String name) throws IOException {
        String blobName = blobName(name);
        BytesReference bytes = serialize(obj);
        blobContainer.writeBlob(blobName, bytes.streamInput(), bytes.length(), false);
    }

}
