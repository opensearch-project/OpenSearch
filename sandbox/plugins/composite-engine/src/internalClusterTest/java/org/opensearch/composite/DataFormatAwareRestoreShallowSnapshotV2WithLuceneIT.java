/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.settings.Settings;

import java.util.List;

/**
 * Variant of {@link DataFormatAwareRestoreShallowSnapshotV2IT} that uses parquet as the primary
 * format with <strong>lucene as a secondary format</strong> (so the Lucene index/ directory
 * contains real segment data files in addition to {@code segments_N}).
 *
 * <p>Inherits all V2 snapshot tests from the base class. The only difference is the index
 * settings: {@code index.composite.secondary_data_formats} is {@code ["lucene"]} instead of
 * the default empty list. This exercises the parquet+lucene-secondary code paths during
 * V2 snapshot creation, restore, and validation.
 *
 * <p>Mirrors the {@code DataFormatAwareReplicationWithLuceneIT} pattern: same test surface,
 * different format combination.
 *
 * @opensearch.experimental
 */
public class DataFormatAwareRestoreShallowSnapshotV2WithLuceneIT extends DataFormatAwareRestoreShallowSnapshotV2IT {

    public DataFormatAwareRestoreShallowSnapshotV2WithLuceneIT(Settings nodeSettings) {
        super(nodeSettings);
    }

    /** Lucene is configured as a secondary format → both formats produce searchable segment files. */
    @Override
    protected List<String> getSecondaryDataFormats() {
        return List.of("lucene");
    }

    /** Drives format-aware on-disk assertions: index/ must contain segment data files beyond segments_N. */
    @Override
    protected boolean hasLuceneSecondary() {
        return true;
    }
}
