/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.RefreshResult;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@ExperimentalApi
public class CatalogSnapshot extends AbstractRefCounted {

    // shard1  - r1 -  f1, f2 -> refresh -> f1,f2
    // f1 - 1
    // f2 - 1
    // search1 - take searcher -> r1 ->
    // f1 - 2
    // f2 - 2
    // shard1 - r2 -> f2, f3 -> refresh ->
    // decref
    // f1  - 1
    // f2 - 1
    // incref
    // f2 - 2
    // f3 - 1
    // search1 is complete
    // f1 - 0
    // f2 - 1
    // f3 - 1


    private Map<String, Collection<FileMetadata>> dfGroupedSearchableFiles = new HashMap<>();
    private final long id;

    public CatalogSnapshot(RefreshResult refreshResult, long id) {
        super("catalog_snapshot");
        refreshResult.getRefreshedFiles().forEach((df, files) -> {
            dfGroupedSearchableFiles.put(df.name(), files);
        });
        this.id = id;
    }

    public Collection<FileMetadata> getSearchableFiles(String df) {
        return dfGroupedSearchableFiles.get(df);
    }

    @Override
    protected void closeInternal() {
        // notify to file deleter, search, etc
    }


    public long getId() {
        return id;
    }

    @Override
    public String toString() {
        return "CatalogSnapshot{" +
            "dfGroupedSearchableFiles=" + dfGroupedSearchableFiles +
            ", id=" + id +
            '}';
    }
}
