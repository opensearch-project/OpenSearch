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

public class CriteriaBasedCompositeDirectory extends FilterDirectory {
    /**
     * Sole constructor, typically called from sub-classes.
     *
     * @param in
     */
    protected CriteriaBasedCompositeDirectory(Directory in) {
        super(in);
    }

    @Override
    public String[] listAll() throws IOException {
        return Arrays.stream(super.listAll()).filter(fileName -> !fileName.startsWith("temp_")).distinct().toArray(String[]::new);
    }
}
