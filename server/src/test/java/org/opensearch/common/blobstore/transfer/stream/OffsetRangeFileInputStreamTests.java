/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer.stream;

import java.io.IOException;

public class OffsetRangeFileInputStreamTests extends ResettableCheckedInputStreamBaseTest {

    @Override
    protected InputStreamContainer provideInputStreamContainer(int offset, long size) throws IOException {
        OffsetRangeFileInputStream offsetRangeFileInputStream = new OffsetRangeFileInputStream(testFile, size, offset);
        return new InputStreamContainer(offsetRangeFileInputStream, () -> {
            try {
                return offsetRangeFileInputStream.getFileChannel().position();
            } catch (IOException e) {
                fail("IOException while fetching fileChannel position");
            }
            return null;
        });
    }
}
