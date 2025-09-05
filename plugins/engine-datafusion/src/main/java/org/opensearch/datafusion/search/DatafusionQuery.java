/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import java.util.Iterator;
import java.util.List;

public class DatafusionQuery {
    private final byte[] substraitBytes;

    // List of Search executors which returns a result iterator which contains row id which can be joined in datafusion
    private final List<SearchExecutor> searchExecutors;

    public DatafusionQuery(byte[] substraitBytes, List<SearchExecutor> searchExecutors) {
        this.substraitBytes = substraitBytes;
        this.searchExecutors = searchExecutors;
    }

    public byte[] getSubstraitBytes() {
        return substraitBytes;
    }

    public List<SearchExecutor> getSearchExecutors() {
        return searchExecutors;
    }
}
