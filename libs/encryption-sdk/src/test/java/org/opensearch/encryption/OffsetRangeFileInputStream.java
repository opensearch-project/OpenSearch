/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.encryption;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class OffsetRangeFileInputStream extends InputStream implements Closeable {
    private final InputStream inputStream;
    private final FileChannel fileChannel;
    private final long actualSizeToRead;
    private final long limit;
    private long counter = 0L;
    private long markPointer;
    private long markCounter;

    public OffsetRangeFileInputStream(Path path, long size, long position) throws IOException {
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        this.fileChannel.position(position);
        this.inputStream = Channels.newInputStream(this.fileChannel);
        long totalLength = this.fileChannel.size();
        this.counter = 0L;
        this.limit = size;
        if (totalLength - position > this.limit) {
            this.actualSizeToRead = this.limit;
        } else {
            this.actualSizeToRead = totalLength - position;
        }

    }

    public int read(byte[] b, int off, int len) throws IOException {
        if (b == null) {
            throw new NullPointerException();
        } else if (off >= 0 && len >= 0 && len <= b.length - off) {
            if (this.fileChannel.position() >= this.fileChannel.size()) {
                return -1;
            } else {
                if (this.fileChannel.position() + (long) len > this.fileChannel.size()) {
                    len = (int) (this.fileChannel.size() - this.fileChannel.position());
                }

                if (this.counter + (long) len > this.limit) {
                    len = (int) (this.limit - this.counter);
                }

                if (len <= 0) {
                    return -1;
                } else {
                    this.inputStream.read(b, off, len);
                    this.counter += (long) len;
                    return len;
                }
            }
        } else {
            throw new IndexOutOfBoundsException();
        }
    }

    public int read() throws IOException {
        if (this.counter++ >= this.limit) {
            return -1;
        } else {
            return this.fileChannel.position() < this.fileChannel.size() ? this.inputStream.read() & 255 : -1;
        }
    }

    public boolean markSupported() {
        return true;
    }

    public synchronized void mark(int readlimit) {
        try {
            this.markPointer = this.fileChannel.position();
        } catch (IOException var3) {
            throw new RuntimeException(var3);
        }

        this.markCounter = this.counter;
    }

    public synchronized void reset() throws IOException {
        this.fileChannel.position(this.markPointer);
        this.counter = this.markCounter;
    }

    public long getFilePointer() throws IOException {
        return this.fileChannel.position();
    }

    public void close() throws IOException {
        this.inputStream.close();
    }
}
