/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

public interface Committer {

    void addLuceneIndexes(CatalogSnapshot catalogSnapshot);

    CommitPoint commit(CatalogSnapshot catalogSnapshot);
}
