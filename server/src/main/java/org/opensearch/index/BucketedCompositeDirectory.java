/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Directory wrapper used to filter out child level directory for context aware enabled indices.
 *
 */
public class BucketedCompositeDirectory extends FilterDirectory {

    public static final String CHILD_DIRECTORY_PREFIX = "temp_";

    protected BucketedCompositeDirectory(Directory in) {
        super(in);
    }

    /**
     * List all files within directory filtering out child level directory.
     * @return files excluding child level directory.
     *
     * @throws IOException in case of I/O error
     */
    @Override
    public String[] listAll() throws IOException {
        return Arrays.stream(super.listAll())
            .filter(fileName -> !fileName.startsWith(CHILD_DIRECTORY_PREFIX))
            .distinct()
            .toArray(String[]::new);
    }
}
