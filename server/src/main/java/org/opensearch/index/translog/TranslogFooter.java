/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
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
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Handles the writing and reading of the translog footer, which contains the checksum of the translog data.
 *
 * Each translog file is structured as follows:
 *
 * 1. Translog header
 * 2. Translog operations
 * 3. Translog footer
 *
 * The footer contains the following information:
 *
 * - Magic number (int): A constant value that identifies the start of the footer.
 * - Algorithm ID (int): The identifier of the checksum algorithm used. Currently, this is always 0.
 * - Checksum (long): The checksum of the entire translog data, calculated using the specified algorithm.
 */
public class TranslogFooter {

    /**
     * FOOTER_LENGTH is the length of the present footer added to translog files.
     * We write 4 bytes for the magic number, 4 bytes for algorithm ID and 8 bytes for the checksum.
     * Therefore, the footer length as 16.
     * */
    private static final int FOOTER_LENGTH = 16;

    /**
     * Returns the length of the translog footer in bytes.
     *
     * @return the length of the translog footer in bytes
     */
    static int footerLength() {
        return FOOTER_LENGTH;
    }

    /**
     * Writes the translog footer to the provided `FileChannel`.
     *
     * @param channel  the `FileChannel` to write the footer to
     * @param checksum the checksum value to be written in the footer
     * @param toSync   whether to force a sync of the written data to the underlying storage
     * @return the byte array containing the written footer data
     * @throws IOException if an I/O error occurs while writing the footer
     */
    static byte[] write(FileChannel channel, long checksum, boolean toSync) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        final OutputStreamDataOutput out = new OutputStreamDataOutput(new OutputStreamStreamOutput(byteArrayOutputStream));

        CodecUtil.writeBEInt(out, CodecUtil.FOOTER_MAGIC);
        CodecUtil.writeBEInt(out, 0);
        CodecUtil.writeBELong(out, checksum);

        Channels.writeToChannel(byteArrayOutputStream.toByteArray(), channel);
        if (toSync) {
            channel.force(false);
        }

        return byteArrayOutputStream.toByteArray();
    }

    /**
     * Reads the checksum value from the footer of the translog file located at the provided `Path`.
     *
     * If the translog file is of an older version and does not have a footer, this method returns `null`.
     *
     * @param path the `Path` to the translog file
     * @return the checksum value from the translog footer, or `null` if the footer is not present
     * @throws IOException if an I/O error occurs while reading the footer
     */
    static Long readChecksum(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            // Read the header and find out if the footer is supported.
            final TranslogHeader header = TranslogHeader.read(path, channel);
            if (header.getTranslogHeaderVersion() < TranslogHeader.VERSION_WITH_FOOTER) {
                return null;
            }

            // Read the footer.
            final long fileSize = channel.size();
            final long footerStart = fileSize - TranslogFooter.footerLength();
            ByteBuffer footer = ByteBuffer.allocate(TranslogFooter.footerLength());
            int bytesRead = Channels.readFromFileChannel(channel, footerStart, footer);
            if (bytesRead != TranslogFooter.footerLength()) {
                throw new IOException(
                    "Read " + bytesRead + " bytes from footer instead of expected " + TranslogFooter.footerLength() + " bytes"
                );
            }
            footer.flip();

            // Validate the footer and return the checksum.
            int magic = footer.getInt();
            if (magic != CodecUtil.FOOTER_MAGIC) {
                throw new IOException("Invalid footer magic number: " + magic);
            }

            int algorithmId = footer.getInt();
            if (algorithmId != 0) {
                throw new IOException("Unsupported checksum algorithm ID: " + algorithmId);
            }

            return footer.getLong();
        }
    }
}
