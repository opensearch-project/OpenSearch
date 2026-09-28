/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.opensearch.common.io.Channels;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Handles the writing and reading of the translog footer, which carries the checksum of the translog
 * content (header + operations) that precedes it.
 *
 * <p>A closed translog generation is laid out as:
 * <pre>
 *   [ header ][ operations ........ ][ footer ]
 *   0        firstOperationOffset   checkpoint.offset   checkpoint.offset + FOOTER_LENGTH
 * </pre>
 *
 * <p>The footer is written <b>after</b> the offset recorded in the generation's checkpoint, i.e. it lives
 * outside the byte range that {@link TranslogReader} ever reads operations from. This keeps the on-disk
 * format readable by nodes that predate the footer (they only read up to {@code checkpoint.offset}), so the
 * translog header version does not need to change and a remote-backed shard can still fail over onto an
 * older node during a rolling upgrade. Its presence is detected structurally: the file must be exactly
 * {@code checkpoint.offset + FOOTER_LENGTH} bytes long and the bytes at {@code checkpoint.offset} must carry
 * the footer magic and a known algorithm id.
 *
 * <p>The footer contains:
 * <ul>
 *   <li>Magic number (int) - {@link CodecUtil#FOOTER_MAGIC}</li>
 *   <li>Algorithm ID (int) - identifies the checksum algorithm, currently always {@link #CHECKSUM_ALGORITHM_CRC32}</li>
 *   <li>Checksum (long) - checksum of {@code [0, checkpoint.offset)}, i.e. header plus operations</li>
 * </ul>
 *
 * <p>The footer is only written for remote-store translogs: its checksum is used by {@link RemoteFsTranslog} to
 * decide whether a generation already present on local disk is byte-identical to the one in the remote store, so
 * that its download can be skipped. A local-only translog is never downloaded and keeps the footer-less layout
 * (file size equal to {@code checkpoint.offset}) it has always had.
 *
 * @opensearch.internal
 */
public final class TranslogFooter {

    /**
     * Identifier for the CRC32 checksum computed by {@link TranslogCheckedContainer}. This is the only algorithm
     * in use today; the field exists so the footer can be evolved without a layout change.
     */
    static final int CHECKSUM_ALGORITHM_CRC32 = 0;

    /**
     * Length of the footer in bytes: 4 (magic) + 4 (algorithm id) + 8 (checksum).
     */
    static final int FOOTER_LENGTH = Integer.BYTES + Integer.BYTES + Long.BYTES;

    private TranslogFooter() {}

    /**
     * Returns the length of the translog footer in bytes.
     */
    static int footerLength() {
        return FOOTER_LENGTH;
    }

    /**
     * Appends the footer at the current position of {@code channel}.
     *
     * @param channel  the translog file channel, positioned at the end of the operations
     * @param checksum the checksum of the bytes preceding the footer (header + operations)
     * @param toSync   whether to fsync the channel after writing the footer
     * @return the footer bytes that were written, so callers can fold them into a running checksum
     * @throws IOException if an I/O error occurs while writing the footer
     */
    static byte[] write(FileChannel channel, long checksum, boolean toSync) throws IOException {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream(FOOTER_LENGTH);
        final OutputStreamDataOutput out = new OutputStreamDataOutput(new OutputStreamStreamOutput(byteArrayOutputStream));
        CodecUtil.writeBEInt(out, CodecUtil.FOOTER_MAGIC);
        CodecUtil.writeBEInt(out, CHECKSUM_ALGORITHM_CRC32);
        CodecUtil.writeBELong(out, checksum);

        final byte[] footer = byteArrayOutputStream.toByteArray();
        assert footer.length == FOOTER_LENGTH : footer.length;
        Channels.writeToChannel(footer, channel);
        if (toSync) {
            channel.force(false);
        }
        return footer;
    }

    /**
     * Reads the content checksum from the footer of a translog generation, if one is present.
     *
     * @param channel          an open channel on the translog file
     * @param checkpointOffset the {@code offset} recorded in the generation's checkpoint, i.e. where the footer starts
     * @return the checksum stored in the footer, or {@code null} if the file carries no footer (a generation written
     *         before footers were introduced, or one whose footer was never completed)
     * @throws IOException if an I/O error occurs while reading
     */
    static Long readChecksum(FileChannel channel, long checkpointOffset) throws IOException {
        if (channel.size() != checkpointOffset + FOOTER_LENGTH) {
            // Either an older generation without a footer (size == offset) or a partially written footer.
            return null;
        }
        final ByteBuffer footer = ByteBuffer.allocate(FOOTER_LENGTH);
        final int bytesRead = Channels.readFromFileChannel(channel, checkpointOffset, footer);
        if (bytesRead != FOOTER_LENGTH) {
            return null;
        }
        footer.flip();
        if (footer.getInt() != CodecUtil.FOOTER_MAGIC) {
            return null;
        }
        if (footer.getInt() != CHECKSUM_ALGORITHM_CRC32) {
            return null;
        }
        return footer.getLong();
    }

    /**
     * Convenience overload of {@link #readChecksum(FileChannel, long)} that opens {@code path} read-only.
     */
    static Long readChecksum(Path path, long checkpointOffset) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            return readChecksum(channel, checkpointOffset);
        }
    }

    /**
     * Reads the content checksum that generation {@code generation} in {@code location} advertises through its
     * footer, locating the footer via the generation's own checkpoint file.
     *
     * @return the footer checksum, or {@code null} if the translog or its checkpoint file is missing, the checkpoint
     *         belongs to a different generation, or the translog carries no complete footer
     * @throws IOException if either file cannot be read, including a checkpoint that fails its own CRC
     */
    public static Long readGenerationChecksum(Path location, long generation) throws IOException {
        Path translogPath = location.resolve(Translog.getFilename(generation));
        Path checkpointPath = location.resolve(Translog.getCommitCheckpointFileName(generation));
        if (Files.isRegularFile(translogPath) == false || Files.isRegularFile(checkpointPath) == false) {
            return null;
        }
        Checkpoint checkpoint = Checkpoint.read(checkpointPath);
        if (checkpoint.generation != generation) {
            return null;
        }
        return readChecksum(translogPath, checkpoint.offset);
    }
}
