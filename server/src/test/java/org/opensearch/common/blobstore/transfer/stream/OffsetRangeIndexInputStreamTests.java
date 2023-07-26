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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

public class OffsetRangeIndexInputStreamTests extends ResettableCheckedInputStreamBaseTest {

    private Directory directory;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        directory = new NIOFSDirectory(testFile.getParent());
    }

    @Override
    protected OffsetRangeInputStream getOffsetRangeInputStream(long size, long position) throws IOException {
        return new OffsetRangeIndexInputStream(directory.openInput(testFile.getFileName().toString(), IOContext.DEFAULT), size, position);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        directory.close();
        super.tearDown();
    }
}
