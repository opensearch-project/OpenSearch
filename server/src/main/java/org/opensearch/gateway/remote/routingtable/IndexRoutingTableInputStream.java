/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.routingtable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class IndexRoutingTableInputStream extends InputStream {

    /**
     * The buffer where data is stored.
     */
    protected byte[] buf;

    /**
     * The number of valid bytes in the buffer.
     */
    protected int count;

    /**
     * The buffer left over from the last fill
     */
    protected byte[] leftOverBuf;

    /**
     * The mark position
     */
    protected int markPos = -1;

    /**
     * The read limit
     */
    protected int markLimit;

    /**
     * The position
     */
    protected int pos;

    private static final int BUFFER_SIZE = 8192;

    private final IndexRoutingTableHeader indexRoutingTableHeader;

    private final Iterator<IndexShardRoutingTable> shardIter;
    private static final Logger logger = LogManager.getLogger(IndexRoutingTableInputStream.class);

    public IndexRoutingTableInputStream(IndexRoutingTable indexRoutingTable, long version, Version nodeVersion) throws IOException {
        this(indexRoutingTable, version, nodeVersion, BUFFER_SIZE);
    }

    public IndexRoutingTableInputStream(IndexRoutingTable indexRoutingTable, long version, Version nodeVersion, int size)
        throws IOException {
        this.buf = new byte[size];
        this.shardIter = indexRoutingTable.iterator();
        this.indexRoutingTableHeader = new IndexRoutingTableHeader(version, indexRoutingTable.getIndex().getName(), nodeVersion);
        logger.info("indexRoutingTable {}, version {}, nodeVersion {}", indexRoutingTable.prettyPrint(), version, nodeVersion);

        initialFill();
    }

    @Override
    public int read() throws IOException {
        if (pos >= count) {
            maybeResizeAndFill();
            if (pos >= count) return -1;
        }
        return buf[pos++] & 0xff;
    }

    private void initialFill() throws IOException {
        BytesReference bytesReference = indexRoutingTableHeader.write();
        buf = bytesReference.toBytesRef().bytes;
        count = bytesReference.length();
        logger.info("bytesReference {} buf {}, count {}", bytesReference , buf, count);

        fill(buf);
    }

    private void fill(byte[] buf) throws IOException {
        if (leftOverBuf != null) {
            if(leftOverBuf.length > buf.length - count) {
                // leftOverBuf has more content than length of buf, so we need to copy only based on buf length and keep the remaining in leftOverBuf.
                System.arraycopy(leftOverBuf, 0, buf, count, buf.length - count);
                byte[] tempLeftOverBuffer =  new byte[leftOverBuf.length - (buf.length - count)];
                System.arraycopy(leftOverBuf, buf.length - count , tempLeftOverBuffer, 0, leftOverBuf.length - (buf.length - count));
                leftOverBuf = tempLeftOverBuffer;
                count = buf.length - count;

            } else {
                System.arraycopy(leftOverBuf, 0, buf, count, leftOverBuf.length);
                count +=  leftOverBuf.length;
                leftOverBuf = null;
            }
        }
        if (count < buf.length && shardIter.hasNext()) {
            IndexShardRoutingTable next = shardIter.next();
            BytesReference bytesRef;
            try (
                BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
                BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(bytesStreamOutput)
            ) {
                IndexShardRoutingTable.Builder.writeTo(next, out);
                // Checksum header
                out.writeInt((int) out.getChecksum());
                out.flush();
                bytesRef = bytesStreamOutput.bytes();
            }
            if (bytesRef.length() < buf.length - count) {
                System.arraycopy(bytesRef.toBytesRef().bytes, 0, buf, count, bytesRef.length());
                count += bytesRef.length();
                leftOverBuf = null;
            } else {
                System.arraycopy(bytesRef.toBytesRef().bytes, 0, buf, count, buf.length - count);
                leftOverBuf = new byte[bytesRef.length() - (buf.length - count)];
                System.arraycopy(bytesRef.toBytesRef().bytes, buf.length - count , leftOverBuf, 0, bytesRef.length() - (buf.length - count));
                count = buf.length;

            }
        }
    }

    private void maybeResizeAndFill() throws IOException {
        byte[] buffer = buf;
        if (markPos == -1) pos = 0; /* no mark: throw away the buffer */
        else if (pos >= buffer.length) { /* no room left in buffer */
            if (markPos > 0) { /* can throw away early part of the buffer */
                int sz = pos - markPos;
                System.arraycopy(buffer, markPos, buffer, 0, sz);
                pos = sz;
                markPos = 0;
            } else if (buffer.length >= markLimit) {
                markPos = -1; /* buffer got too big, invalidate mark */
                pos = 0; /* drop buffer contents */
            } else { /* grow buffer */
                int nsz = markLimit + 1; //NEED TO CHECK THIS
                byte[] nbuf = new byte[nsz];
                System.arraycopy(buffer, 0, nbuf, 0, pos);
                buffer = nbuf;
            }
        }
        count = pos;
        fill(buffer);
    }

    @Override
    public void mark(int readlimit) {
        markLimit = readlimit;
        markPos = pos;
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public void reset() throws IOException {
        if (markPos < 0) throw new IOException("Resetting to invalid mark");
        pos = markPos;
    }
}
