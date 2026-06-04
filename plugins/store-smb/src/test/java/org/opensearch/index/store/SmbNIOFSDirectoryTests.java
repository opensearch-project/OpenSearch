/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * SMB Tests using NIO FileSystem as index store type.
 */
public class SmbNIOFSDirectoryTests extends OpenSearchBaseDirectoryTestCase {

    @Override
    protected Directory getDirectory(Path file) throws IOException {
        return new SmbDirectoryWrapper(new NIOFSDirectory(file));
    }

    @Override
    public void testCreateOutputForExistingFile() throws IOException {
        /*
          This test is disabled because {@link SmbDirectoryWrapper} opens existing file
          with an explicit StandardOpenOption.TRUNCATE_EXISTING option.
         */
    }
}
