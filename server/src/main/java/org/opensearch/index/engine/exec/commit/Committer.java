/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.Segment;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Committer extends Closeable {

    void addLuceneIndexes(List<Segment> catalogSnapshot) throws IOException;

    CommitPoint commit(Iterable<Map.Entry<String, String>> commitData, CatalogSnapshot catalogSnapshot);

    Map<String, String> getLastCommittedData();

    CommitStats getCommitStats();

    SafeCommitInfo getSafeCommitInfo();
}
