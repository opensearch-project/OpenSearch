/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.directory;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.index.store.BaseRemoteSegmentStoreDirectoryTests;
import org.opensearch.index.store.RemoteDirectory;

import java.io.IOException;

import static java.lang.Math.min;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Extension of the existing BaseRemoteSegmentStoreDirectoryTests with additional
 * helper methods for writable warm tiered storage tests.
 */
public abstract class TieredStorageBaseTestCase extends BaseRemoteSegmentStoreDirectoryTests {

    protected static final int EIGHT_MB = 1024 * 1024 * 8;
    protected static final int FILE_CACHE_CAPACITY = 10000000;

    protected void populateData() throws IOException {
        long fileLength = remoteSegmentStoreDirectory.fileLength("_0.si");
        IndexInput fullIndexInput = new ByteArrayIndexInput("full", new byte[(int) fileLength]);
        IndexInput blockIndexInput = fullIndexInput.slice("slice", 0, min(fileLength, EIGHT_MB));
        when(((RemoteDirectory) remoteDataDirectory).openBlockInput(anyString(), anyLong(), anyLong(), anyLong(), any())).thenReturn(
            blockIndexInput
        );
        when(((RemoteDirectory) remoteDataDirectory).openInput(anyString(), anyLong(), any())).thenReturn(fullIndexInput);
    }

    protected void syncLocalAndRemoteForFile(FSDirectory localDirectory, String fileName) throws IOException {
        try (
            IndexInput input = remoteSegmentStoreDirectory.openInput(fileName, IOContext.DEFAULT);
            IndexOutput output = localDirectory.createOutput(fileName, IOContext.DEFAULT)
        ) {
            byte[] buffer = new byte[8192];
            long len = input.length();
            long pos = 0;
            while (pos < len) {
                int size = (int) Math.min(buffer.length, len - pos);
                input.readBytes(buffer, 0, size);
                output.writeBytes(buffer, 0, size);
                pos += size;
            }
        }
    }

    public static byte[] createData() {
        final byte[] data = new byte[EIGHT_MB];
        data[0] = data[EIGHT_MB - 1] = 7;
        return data;
    }
}
