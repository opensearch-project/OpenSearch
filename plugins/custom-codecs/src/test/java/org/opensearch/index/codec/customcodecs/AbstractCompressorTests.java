/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodecs;

import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.Decompressor;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.tests.util.LineFileDocs;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

/**
 * Test cases for compressors (based on {@See org.opensearch.common.compress.DeflateCompressTests}).
 */
public abstract class AbstractCompressorTests extends OpenSearchTestCase {

    abstract Compressor compressor();

    abstract Decompressor decompressor();

    public void testEmpty() throws IOException {
        final byte[] bytes = "".getBytes(StandardCharsets.UTF_8);
        doTest(bytes);
    }

    public void testShortLiterals() throws IOException {
        final byte[] bytes = "1234567345673456745608910123".getBytes(StandardCharsets.UTF_8);
        doTest(bytes);
    }

    public void testRandom() throws IOException {
        Random r = random();
        for (int i = 0; i < 10; i++) {
            final byte[] bytes = new byte[TestUtil.nextInt(r, 1, 100000)];
            r.nextBytes(bytes);
            doTest(bytes);
        }
    }

    public void testLineDocs() throws IOException {
        Random r = random();
        LineFileDocs lineFileDocs = new LineFileDocs(r);
        for (int i = 0; i < 10; i++) {
            int numDocs = TestUtil.nextInt(r, 1, 200);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int j = 0; j < numDocs; j++) {
                String s = lineFileDocs.nextDoc().get("body");
                bos.write(s.getBytes(StandardCharsets.UTF_8));
            }
            doTest(bos.toByteArray());
        }
        lineFileDocs.close();
    }

    public void testRepetitionsL() throws IOException {
        Random r = random();
        for (int i = 0; i < 10; i++) {
            int numLongs = TestUtil.nextInt(r, 1, 10000);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            long theValue = r.nextLong();
            for (int j = 0; j < numLongs; j++) {
                if (r.nextInt(10) == 0) {
                    theValue = r.nextLong();
                }
                bos.write((byte) (theValue >>> 56));
                bos.write((byte) (theValue >>> 48));
                bos.write((byte) (theValue >>> 40));
                bos.write((byte) (theValue >>> 32));
                bos.write((byte) (theValue >>> 24));
                bos.write((byte) (theValue >>> 16));
                bos.write((byte) (theValue >>> 8));
                bos.write((byte) theValue);
            }
            doTest(bos.toByteArray());
        }
    }

    public void testRepetitionsI() throws IOException {
        Random r = random();
        for (int i = 0; i < 10; i++) {
            int numInts = TestUtil.nextInt(r, 1, 20000);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int theValue = r.nextInt();
            for (int j = 0; j < numInts; j++) {
                if (r.nextInt(10) == 0) {
                    theValue = r.nextInt();
                }
                bos.write((byte) (theValue >>> 24));
                bos.write((byte) (theValue >>> 16));
                bos.write((byte) (theValue >>> 8));
                bos.write((byte) theValue);
            }
            doTest(bos.toByteArray());
        }
    }

    public void testRepetitionsS() throws IOException {
        Random r = random();
        for (int i = 0; i < 10; i++) {
            int numShorts = TestUtil.nextInt(r, 1, 40000);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            short theValue = (short) r.nextInt(65535);
            for (int j = 0; j < numShorts; j++) {
                if (r.nextInt(10) == 0) {
                    theValue = (short) r.nextInt(65535);
                }
                bos.write((byte) (theValue >>> 8));
                bos.write((byte) theValue);
            }
            doTest(bos.toByteArray());
        }
    }

    public void testMixed() throws IOException {
        Random r = random();
        LineFileDocs lineFileDocs = new LineFileDocs(r);
        for (int i = 0; i < 2; ++i) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            int prevInt = r.nextInt();
            long prevLong = r.nextLong();
            while (bos.size() < 400000) {
                switch (r.nextInt(4)) {
                    case 0:
                        addInt(r, prevInt, bos);
                        break;
                    case 1:
                        addLong(r, prevLong, bos);
                        break;
                    case 2:
                        addString(lineFileDocs, bos);
                        break;
                    case 3:
                        addBytes(r, bos);
                        break;
                    default:
                        throw new IllegalStateException("Random is broken");
                }
            }
            doTest(bos.toByteArray());
        }
    }

    private void addLong(Random r, long prev, ByteArrayOutputStream bos) {
        long theValue = prev;
        if (r.nextInt(10) != 0) {
            theValue = r.nextLong();
        }
        bos.write((byte) (theValue >>> 56));
        bos.write((byte) (theValue >>> 48));
        bos.write((byte) (theValue >>> 40));
        bos.write((byte) (theValue >>> 32));
        bos.write((byte) (theValue >>> 24));
        bos.write((byte) (theValue >>> 16));
        bos.write((byte) (theValue >>> 8));
        bos.write((byte) theValue);
    }

    private void addInt(Random r, int prev, ByteArrayOutputStream bos) {
        int theValue = prev;
        if (r.nextInt(10) != 0) {
            theValue = r.nextInt();
        }
        bos.write((byte) (theValue >>> 24));
        bos.write((byte) (theValue >>> 16));
        bos.write((byte) (theValue >>> 8));
        bos.write((byte) theValue);
    }

    private void addString(LineFileDocs lineFileDocs, ByteArrayOutputStream bos) throws IOException {
        String s = lineFileDocs.nextDoc().get("body");
        bos.write(s.getBytes(StandardCharsets.UTF_8));
    }

    private void addBytes(Random r, ByteArrayOutputStream bos) throws IOException {
        byte bytes[] = new byte[TestUtil.nextInt(r, 1, 10000)];
        r.nextBytes(bytes);
        bos.write(bytes);
    }

    private void doTest(byte[] bytes) throws IOException {
        final int length = bytes.length;

        ByteBuffersDataInput in = new ByteBuffersDataInput(List.of(ByteBuffer.wrap(bytes)));
        ByteBuffersDataOutput out = new ByteBuffersDataOutput();

        // let's compress
        Compressor compressor = compressor();
        compressor.compress(in, out);
        byte[] compressed = out.toArrayCopy();

        // let's decompress
        BytesRef outbytes = new BytesRef();
        Decompressor decompressor = decompressor();
        decompressor.decompress(new ByteArrayDataInput(compressed), length, 0, length, outbytes);

        // get the uncompressed array out of outbytes
        byte[] restored = new byte[outbytes.length];
        System.arraycopy(outbytes.bytes, 0, restored, 0, outbytes.length);

        assertArrayEquals(bytes, restored);
    }

}
