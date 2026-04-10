/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.commit;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.CatalogSnapshotDeletionPolicy;
import org.opensearch.index.store.Store;

import java.util.Optional;

/**
 * Initialization parameters for a {@link Committer}.
 * Carries the shard path, index settings, engine configuration, and store needed to set up the backing store.
 *
 * @param indexSettings the index-level settings
 * @param engineConfig  the engine configuration (nullable — may be absent in tests or standalone mode)
 * @param store         the shard's store providing the Lucene directory (nullable — may be absent in tests)
 * @param deletionPolicy the deletion policy for safe commit trimming on startup (empty if not needed)
 * @opensearch.experimental
 */
@ExperimentalApi
public record CommitterConfig(IndexSettings indexSettings, EngineConfig engineConfig, Store store, Optional<
    CatalogSnapshotDeletionPolicy> deletionPolicy) {
}
