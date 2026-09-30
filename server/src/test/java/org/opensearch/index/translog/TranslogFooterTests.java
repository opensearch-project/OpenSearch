/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.lucene.codecs.CodecUtil;
import org.opensearch.common.UUIDs;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class TranslogFooterTests extends OpenSearchTestCase {

    /**
     * Writes header + a few operation bytes and returns the offset at which a footer would start,
     * i.e. what the generation's checkpoint would record as {@code offset}.
     */
    private static long writeHeaderAndOperations(FileChannel channel) throws IOException {
        TranslogHeader header = new TranslogHeader(UUIDs.randomBase64UUID(), randomNonNegativeLong());
        header.write(channel, true);
        channel.write(ByteBuffer.wrap(randomByteArrayOfLength(randomIntBetween(1, 128))));
        return channel.position();
    }

    public void testWriteProducesFixedLengthFooterAfterCheckpointOffset() throws IOException {
        Path translogPath = createTempFile();
        long expectedChecksum = randomLong();
        long checkpointOffset;
        byte[] footer;
        try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
            checkpointOffset = writeHeaderAndOperations(channel);
            footer = TranslogFooter.write(channel, expectedChecksum, randomBoolean());
            assertEquals(TranslogFooter.footerLength(), footer.length);
            assertEquals(checkpointOffset + TranslogFooter.footerLength(), channel.size());
        }

        ByteBuffer footerBuffer = ByteBuffer.wrap(footer);
        assertEquals(CodecUtil.FOOTER_MAGIC, footerBuffer.getInt());
        assertEquals(TranslogFooter.CHECKSUM_ALGORITHM_CRC32, footerBuffer.getInt());
        assertEquals(expectedChecksum, footerBuffer.getLong());

        // The returned bytes are exactly what landed on disk.
        byte[] onDisk = Files.readAllBytes(translogPath);
        assertArrayEquals(footer, java.util.Arrays.copyOfRange(onDisk, (int) checkpointOffset, onDisk.length));
    }

    public void testReadChecksumRoundTrip() throws IOException {
        Path translogPath = createTempFile();
        long expectedChecksum = randomLong();
        long checkpointOffset;
        try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
            checkpointOffset = writeHeaderAndOperations(channel);
            TranslogFooter.write(channel, expectedChecksum, true);
        }
        assertEquals(Long.valueOf(expectedChecksum), TranslogFooter.readChecksum(translogPath, checkpointOffset));
        try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.READ)) {
            assertEquals(Long.valueOf(expectedChecksum), TranslogFooter.readChecksum(channel, checkpointOffset));
        }
    }

    /**
     * A generation written before footers were introduced ends exactly at the checkpoint offset and must be
     * reported as footer-less rather than misread.
     */
    public void testReadChecksumReturnsNullWithoutFooter() throws IOException {
        Path translogPath = createTempFile();
        long checkpointOffset;
        try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
            checkpointOffset = writeHeaderAndOperations(channel);
        }
        assertNull(TranslogFooter.readChecksum(translogPath, checkpointOffset));
    }

    /**
     * Anything that is not exactly one well-formed footer past the checkpoint offset is treated as "no footer":
     * a truncated footer, extra trailing bytes, or trailing bytes that happen to be footer-sized but carry the
     * wrong magic or algorithm id.
     */
    public void testReadChecksumRejectsMalformedTrailer() throws IOException {
        // Truncated footer.
        {
            Path translogPath = createTempFile();
            long checkpointOffset;
            try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
                checkpointOffset = writeHeaderAndOperations(channel);
                TranslogFooter.write(channel, randomLong(), true);
                channel.truncate(channel.size() - randomIntBetween(1, TranslogFooter.footerLength() - 1));
            }
            assertNull(TranslogFooter.readChecksum(translogPath, checkpointOffset));
        }
        // Extra bytes after the footer.
        {
            Path translogPath = createTempFile();
            long checkpointOffset;
            try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
                checkpointOffset = writeHeaderAndOperations(channel);
                TranslogFooter.write(channel, randomLong(), true);
                channel.write(ByteBuffer.wrap(new byte[] { 1 }));
            }
            assertNull(TranslogFooter.readChecksum(translogPath, checkpointOffset));
        }
        // Footer-sized trailer with the wrong magic.
        {
            Path translogPath = createTempFile();
            long checkpointOffset;
            try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
                checkpointOffset = writeHeaderAndOperations(channel);
                ByteBuffer bogus = ByteBuffer.allocate(TranslogFooter.footerLength());
                bogus.putInt(CodecUtil.FOOTER_MAGIC + 1).putInt(TranslogFooter.CHECKSUM_ALGORITHM_CRC32).putLong(randomLong());
                bogus.flip();
                channel.write(bogus);
            }
            assertNull(TranslogFooter.readChecksum(translogPath, checkpointOffset));
        }
        // Footer-sized trailer with an unknown algorithm id.
        {
            Path translogPath = createTempFile();
            long checkpointOffset;
            try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
                checkpointOffset = writeHeaderAndOperations(channel);
                ByteBuffer bogus = ByteBuffer.allocate(TranslogFooter.footerLength());
                bogus.putInt(CodecUtil.FOOTER_MAGIC).putInt(TranslogFooter.CHECKSUM_ALGORITHM_CRC32 + 1).putLong(randomLong());
                bogus.flip();
                channel.write(bogus);
            }
            assertNull(TranslogFooter.readChecksum(translogPath, checkpointOffset));
        }
    }
}
