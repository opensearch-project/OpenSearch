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
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.opensearch.common.UUIDs;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class TranslogFooterTests extends OpenSearchTestCase {

    /**
     * testTranslogFooterWrite verifies the functionality of TranslogFooter.write() method
     * wherein we write the footer to the translog file.
     * */
    public void testTranslogFooterWrite() throws IOException {
        Path translogPath = createTempFile();
        try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
            // Write a translog header
            TranslogHeader header = new TranslogHeader(UUIDs.randomBase64UUID(), randomNonNegativeLong());
            header.write(channel, true);

            // Write some translog operations
            byte[] operationBytes = new byte[] { 1, 2, 3, 4 };
            channel.write(ByteBuffer.wrap(operationBytes));

            // Write the translog footer
            long expectedChecksum = 0x1234567890ABCDEFL;
            byte[] footer = TranslogFooter.write(channel, expectedChecksum, true);

            // Verify the footer contents
            ByteBuffer footerBuffer = ByteBuffer.wrap(footer);
            assertEquals(CodecUtil.FOOTER_MAGIC, footerBuffer.getInt());
            assertEquals(0, footerBuffer.getInt());
            assertEquals(expectedChecksum, footerBuffer.getLong());

            // Verify that the footer was written to the channel
            assertEquals(footer.length, channel.size() - (header.sizeInBytes() + operationBytes.length));
        }
    }

    /**
     * testTranslogFooterReadChecksum verifies the behavior of the TranslogFooter.readChecksum() method,
     * which reads the checksum from the footer of a translog file.
     * */
    public void testTranslogFooterReadChecksum() throws IOException {
        long expectedChecksum = 0x1234567890ABCDEFL;
        Path translogPath = createTempFile();
        try (FileChannel channel = FileChannel.open(translogPath, StandardOpenOption.WRITE)) {
            // Write a translog header
            TranslogHeader header = new TranslogHeader(UUIDs.randomBase64UUID(), randomNonNegativeLong());
            header.write(channel, true);

            // Write some translog operations
            byte[] operationBytes = new byte[] { 1, 2, 3, 4 };
            channel.write(ByteBuffer.wrap(operationBytes));

            // Write the translog footer.
            TranslogFooter.write(channel, expectedChecksum, true);
        }

        // Verify that the checksum can be read correctly
        Long actualChecksum = TranslogFooter.readChecksum(translogPath);
        assert actualChecksum != null;
        assertEquals(expectedChecksum, actualChecksum.longValue());
    }
}
