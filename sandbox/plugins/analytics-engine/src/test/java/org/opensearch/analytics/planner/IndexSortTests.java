/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Optional;

public class IndexSortTests extends OpenSearchTestCase {

    private static final IndexSort.Key TS_ASC = new IndexSort.Key("ts", false);
    private static final IndexSort.Key TS_DESC = new IndexSort.Key("ts", true);
    private static final IndexSort.Key ID_ASC = new IndexSort.Key("id", false);
    private static final IndexSort.Key ID_DESC = new IndexSort.Key("id", true);

    // ---- of() ----

    public void testOf_noIndexSort() {
        assertEquals(Optional.empty(), IndexSort.of(index("i", Settings.EMPTY)));
    }

    public void testOf_fieldsWithoutOrderDefaultToAscending() {
        IndexSort sort = IndexSort.of(index("i", Settings.builder().putList("index.sort.field", "ts", "id").build())).orElseThrow();
        assertEquals(List.of(TS_ASC, ID_ASC), sort.keys());
    }

    public void testOf_explicitOrders() {
        IndexSort sort = IndexSort.of(index("i", sortSettings(List.of("ts", "id"), List.of("desc", "asc")))).orElseThrow();
        assertEquals(List.of(TS_DESC, ID_ASC), sort.keys());
    }

    public void testOf_orderListLengthMismatchIsUnknown() {
        // Would be rejected by IndexSortConfig at creation; the planner must not index past the order list.
        assertEquals(Optional.empty(), IndexSort.of(index("i", sortSettings(List.of("ts", "id"), List.of("desc")))));
        assertEquals(Optional.empty(), IndexSort.of(index("i", sortSettings(List.of("ts"), List.of("desc", "asc")))));
    }

    public void testOf_keysAreImmutable() {
        IndexSort sort = IndexSort.of(index("i", sortSettings(List.of("ts"), List.of("asc")))).orElseThrow();
        expectThrows(UnsupportedOperationException.class, () -> sort.keys().add(ID_ASC));
    }

    // ---- commonTo() ----

    public void testCommonTo_emptyList() {
        assertEquals(Optional.empty(), IndexSort.commonTo(List.of()));
    }

    public void testCommonTo_allSame() {
        Settings s = sortSettings(List.of("ts"), List.of("desc"));
        assertEquals(Optional.of(new IndexSort(List.of(TS_DESC))), IndexSort.commonTo(List.of(index("a", s), index("b", s))));
    }

    public void testCommonTo_differentDirection() {
        assertEquals(
            Optional.empty(),
            IndexSort.commonTo(
                List.of(index("a", sortSettings(List.of("ts"), List.of("desc"))), index("b", sortSettings(List.of("ts"), List.of("asc"))))
            )
        );
    }

    public void testCommonTo_oneIndexUnsorted() {
        assertEquals(
            Optional.empty(),
            IndexSort.commonTo(List.of(index("a", sortSettings(List.of("ts"), List.of("asc"))), index("b", Settings.EMPTY)))
        );
        assertEquals(
            Optional.empty(),
            IndexSort.commonTo(List.of(index("a", Settings.EMPTY), index("b", sortSettings(List.of("ts"), List.of("asc")))))
        );
    }

    // ---- serves() ----

    public void testServes_exactMatch() {
        assertTrue(new IndexSort(List.of(TS_DESC)).serves(List.of(TS_DESC)));
    }

    public void testServes_fullyReversed() {
        assertTrue(new IndexSort(List.of(TS_DESC)).serves(List.of(TS_ASC)));
        assertTrue(new IndexSort(List.of(TS_DESC, ID_ASC)).serves(List.of(TS_ASC, ID_DESC)));
    }

    public void testServes_prefix() {
        IndexSort sort = new IndexSort(List.of(TS_DESC, ID_ASC));
        assertTrue(sort.serves(List.of(TS_DESC)));
        assertTrue(sort.serves(List.of(TS_ASC)));
    }

    public void testServes_rejectsNonLeadingKey() {
        assertFalse(new IndexSort(List.of(TS_DESC, ID_ASC)).serves(List.of(ID_ASC)));
    }

    public void testServes_rejectsPartiallyReversed() {
        assertFalse(new IndexSort(List.of(TS_DESC, ID_ASC)).serves(List.of(TS_DESC, ID_DESC)));
        assertFalse(new IndexSort(List.of(TS_DESC, ID_ASC)).serves(List.of(TS_ASC, ID_ASC)));
    }

    public void testServes_rejectsLongerCollation() {
        assertFalse(new IndexSort(List.of(TS_DESC)).serves(List.of(TS_DESC, ID_ASC)));
    }

    public void testServes_rejectsEmptyCollation() {
        assertFalse(new IndexSort(List.of(TS_DESC)).serves(List.of()));
    }

    public void testServes_fieldNamesAreCaseSensitive() {
        // Field names are compared as mapped; both sides come from the same mapping so no normalization is applied.
        assertFalse(new IndexSort(List.of(TS_DESC)).serves(List.of(new IndexSort.Key("TS", true))));
    }

    // ---- helpers ----

    private static Settings sortSettings(List<String> fields, List<String> orders) {
        return Settings.builder().putList("index.sort.field", fields).putList("index.sort.order", orders).build();
    }

    private static IndexMetadata index(String name, Settings extra) {
        return IndexMetadata.builder(name)
            .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT.id).put(extra))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }
}
