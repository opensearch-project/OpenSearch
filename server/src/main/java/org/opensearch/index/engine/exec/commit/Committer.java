/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;

public interface Committer extends Closeable {

    void addLuceneIndexes(CatalogSnapshot catalogSnapshot);

    CommitPoint commit(Iterable<Map.Entry<String, String>> commitData, CatalogSnapshot catalogSnapshot);

    Map<String, String> getLastCommittedData() throws IOException;

    Optional<CatalogSnapshot> readLastCommittedCatalogSnapshot() throws IOException;

    SequenceNumbers.CommitInfo loadSeqNoInfoFromLastCommit() throws IOException;
}
