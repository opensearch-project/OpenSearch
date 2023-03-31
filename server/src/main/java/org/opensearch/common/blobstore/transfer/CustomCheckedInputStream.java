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
import java.util.zip.Checksum;

public class CustomCheckedInputStream extends InputStream {
    private Checksum cksum;
    private InputStream in;
    private String filename;
    private FileChannel fileChannel;
    private IndexInput indexInput;

    /**
     * Creates an input stream using the specified Checksum.
     * @param in the input stream
     * @param cksum the Checksum
     */
    public CustomCheckedInputStream(InputStream in, Checksum cksum, String fileName, FileChannel fileChannel) {
        this.cksum = cksum;
        this.in = in;
        this.filename = fileName;
        this.fileChannel = fileChannel;
    }

    public CustomCheckedInputStream(InputStream in, Checksum cksum, String fileName, IndexInput indexInput) {
        this.cksum = cksum;
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

    /**
     * Reads a byte. Will block if no input is available.
     * @return the byte read, or -1 if the end of the stream is reached.
     * @exception IOException if an I/O error has occurred
     */
    public int read() throws IOException {
        int b = in.read();
        if (b != -1) {
            cksum.update(b);
        }
        System.out.println("Reading single byte" +  ", filename: " + filename + ", position: " + getPosition() + ", checksum: " + getChecksum().getValue());
        return b;
    }

    /**
     * Reads into an array of bytes. If <code>len</code> is not zero, the method
     * blocks until some input is available; otherwise, no
     * bytes are read and <code>0</code> is returned.
     * @param buf the buffer into which the data is read
     * @param off the start offset in the destination array <code>b</code>
     * @param len the maximum number of bytes read
     * @return    the actual number of bytes read, or -1 if the end
     *            of the stream is reached.
     * @exception  NullPointerException If <code>buf</code> is <code>null</code>.
     * @exception  IndexOutOfBoundsException If <code>off</code> is negative,
     * <code>len</code> is negative, or <code>len</code> is greater than
     * <code>buf.length - off</code>
     * @exception IOException if an I/O error has occurred
     */
    public int read(byte[] buf, int off, int len) throws IOException {
        len = in.read(buf, off, len);
        if (len != -1) {
            cksum.update(buf, off, len);
        }
        System.out.println("Reading offset: " + off + ", length: " + len + ", filename: " + filename + ", position: " + getPosition() + ", checksum: " + getChecksum().getValue());
        return len;
    }

    /**
     * Skips specified number of bytes of input.
     * @param n the number of bytes to skip
     * @return the actual number of bytes skipped
     * @exception IOException if an I/O error has occurred
     */
    public long skip(long n) throws IOException {
        byte[] buf = new byte[512];
        long total = 0;
        while (total < n) {
            long len = n - total;
            len = read(buf, 0, len < buf.length ? (int)len : buf.length);
            if (len == -1) {
                return total;
            }
            total += len;
        }
        return total;
    }

    /**
     * Returns the Checksum for this input stream.
     * @return the Checksum value
     */
    public Checksum getChecksum() {
        return cksum;
    }
}
