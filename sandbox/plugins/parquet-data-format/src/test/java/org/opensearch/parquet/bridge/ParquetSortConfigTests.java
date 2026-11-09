/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/**
 * Verifies that {@link ParquetSortConfig} derives the per-field multi-value
 * reduction mode ({@code maxSortModes}, {@code true = MAX / false = MIN}) exactly
 * like {@code IndexSortConfig#buildIndexSort}: an explicit {@code index.sort.mode}
 * overrides the direction default, otherwise the mode defaults to MAX for a
 * descending field and MIN for an ascending field.
 */
public class ParquetSortConfigTests extends OpenSearchTestCase {

    // ── deriveMaxSortModes: default derivation from order ────────────────

    public void testDefaultModeAscendingIsMin() {
        assertEquals(List.of(false), ParquetSortConfig.deriveMaxSortModes(1, List.of(SortOrder.ASC), List.of()));
    }

    public void testDefaultModeDescendingIsMax() {
        assertEquals(List.of(true), ParquetSortConfig.deriveMaxSortModes(1, List.of(SortOrder.DESC), List.of()));
    }

    public void testExplicitModeOverridesAscendingDefault() {
        assertEquals(List.of(true), ParquetSortConfig.deriveMaxSortModes(1, List.of(SortOrder.ASC), List.of(MultiValueMode.MAX)));
    }

    public void testExplicitModeOverridesDescendingDefault() {
        assertEquals(List.of(false), ParquetSortConfig.deriveMaxSortModes(1, List.of(SortOrder.DESC), List.of(MultiValueMode.MIN)));
    }

    public void testMultiFieldModes() {
        assertEquals(
            List.of(false, true, true),
            ParquetSortConfig.deriveMaxSortModes(
                3,
                List.of(SortOrder.ASC, SortOrder.DESC, SortOrder.ASC),
                List.of(MultiValueMode.MIN, MultiValueMode.MAX, MultiValueMode.MAX)
            )
        );
    }

    public void testOmittedOrdersAndModesDefaultEveryFieldToMin() {
        assertEquals(List.of(false, false), ParquetSortConfig.deriveMaxSortModes(2, List.of(), List.of()));
    }

    public void testNoSortFieldsYieldNoModes() {
        assertEquals(List.of(), ParquetSortConfig.deriveMaxSortModes(0, List.of(), List.of()));
    }

    // ── Full constructor from IndexSettings ──────────────────────────────

    private IndexSettings indexSettings(Settings.Builder extra) {
        Settings settings = extra.put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT).build();
        return IndexSettingsModule.newIndexSettings("test", settings);
    }

    public void testConstructorDefaultsDescendingToMax() {
        // index.sort.mode omitted; order = desc => MAX.
        ParquetSortConfig config = new ParquetSortConfig(
            indexSettings(Settings.builder().put("index.sort.field", "ts").put("index.sort.order", "desc"))
        );
        assertEquals(List.of("ts"), config.sortColumns());
        assertEquals(List.of(true), config.reverseSorts());
        assertEquals(List.of(true), config.maxSortModes());
    }

    public void testConstructorDefaultsAscendingToMin() {
        // No order or mode set => ASC default => MIN.
        ParquetSortConfig config = new ParquetSortConfig(indexSettings(Settings.builder().put("index.sort.field", "ts")));
        assertEquals(List.of("ts"), config.sortColumns());
        assertEquals(List.of(false), config.maxSortModes());
    }

    public void testConstructorExplicitModeWins() {
        // order = asc but explicit mode = max => MAX.
        ParquetSortConfig config = new ParquetSortConfig(
            indexSettings(Settings.builder().put("index.sort.field", "ts").put("index.sort.order", "asc").put("index.sort.mode", "max"))
        );
        assertEquals(List.of(false), config.reverseSorts());
        assertEquals(List.of(true), config.maxSortModes());
    }

    public void testConstructorMultiFieldMixed() {
        ParquetSortConfig config = new ParquetSortConfig(
            indexSettings(
                Settings.builder()
                    .putList("index.sort.field", "a", "b")
                    .putList("index.sort.order", "asc", "desc")
                    .putList("index.sort.mode", "max", "min")
            )
        );
        assertEquals(List.of("a", "b"), config.sortColumns());
        assertEquals(List.of(false, true), config.reverseSorts());
        // a: explicit max => true; b: explicit min => false.
        assertEquals(List.of(true, false), config.maxSortModes());
    }

    public void testEmptyConfigHasEmptyModes() {
        assertEquals(List.of(), ParquetSortConfig.empty().maxSortModes());
    }
}
