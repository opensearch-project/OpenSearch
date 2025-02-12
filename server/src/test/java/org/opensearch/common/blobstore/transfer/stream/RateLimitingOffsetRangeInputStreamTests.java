/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.transfer.stream;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RateLimiter;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class RateLimitingOffsetRangeInputStreamTests extends ResettableCheckedInputStreamBaseTest {

    private Directory directory;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        directory = new NIOFSDirectory(testFile.getParent());
    }

    @Override
    protected OffsetRangeInputStream getOffsetRangeInputStream(long size, long position) throws IOException {
        return new RateLimitingOffsetRangeInputStream(
            new OffsetRangeIndexInputStream(directory.openInput(testFile.getFileName().toString(), IOContext.DEFAULT), size, position),
            () -> new RateLimiter.SimpleRateLimiter(randomIntBetween(10, 20)),
            (t) -> {}
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        directory.close();
        super.tearDown();
    }
}
