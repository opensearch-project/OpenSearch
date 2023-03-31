/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer;

import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.zip.CheckedInputStream;
import java.util.zip.Checksum;

public class CustomCheckedInputStreamExtended extends ResettableCheckedInputStream {

    private InputStream in;
    private String filename;
    private FileChannel fileChannel;
    private IndexInput indexInput;
    private long position = -1;

    /**
     * Creates an input stream using the specified Checksum.
     *
     * @param in    the input stream
     */
    public CustomCheckedInputStreamExtended(InputStream in, String fileName, FileChannel fileChannel) {
        super(in);
        this.in = in;
        this.filename = fileName;
        this.fileChannel = fileChannel;
    }

    public CustomCheckedInputStreamExtended(InputStream in, String fileName, IndexInput indexInput) {
        super(in);
        this.in = in;
        this.filename = fileName;
        this.indexInput = indexInput;
    }

    public long getPosition() throws IOException {
        if (fileChannel != null) {
            return fileChannel.position();
        } else {
            return indexInput.getFilePointer();
        }
    }

    @Override
    public int read() throws IOException {
        if (getPosition() == position) {
            System.out.println();
        }
        position = getPosition();
        int r = super.read();
        System.out.println("Reading single byte" +  ", filename: " + filename + ", position: " + getPosition() + ", checksum: " + getChecksum());
        return r;
    }

    @Override
    public int read(byte[] buf, int off, int len) throws IOException {
        if (getPosition() == position) {
            System.out.println();
        }
        position = getPosition();
        int r = super.read(buf, off, len);
        System.out.println("Reading offset: " + off + ", length: " + len + ", filename: " + filename + ", position: " + getPosition() + ", checksum: " + getChecksum());
        return r;
    }
}
