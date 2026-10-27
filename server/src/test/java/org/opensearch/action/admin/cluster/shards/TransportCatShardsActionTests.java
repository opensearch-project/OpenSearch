/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.test.OpenSearchTestCase;

import java.util.EnumSet;

import static org.opensearch.action.support.IndicesOptions.WildcardStates.HIDDEN;
import static org.opensearch.action.support.IndicesOptions.WildcardStates.OPEN;

public class TransportCatShardsActionTests extends OpenSearchTestCase {

    private static final IndicesOptions STATS_DEFAULTS = new IndicesStatsRequest().indicesOptions();

    public void testDefaultExpandWildcardsKeepsStatsDefaults() {
        IndicesOptions requested = new CatShardsRequest().getIndicesOptions();
        assertEquals(STATS_DEFAULTS, TransportCatShardsAction.statsIndicesOptions(requested, STATS_DEFAULTS));
    }

    public void testStatsExpandOpenAndHiddenButNeverClosed() {
        IndicesOptions requested = IndicesOptions.fromParameters("all", null, null, null, IndicesOptions.lenientExpandOpen());
        IndicesOptions stats = TransportCatShardsAction.statsIndicesOptions(requested, STATS_DEFAULTS);
        assertEquals(EnumSet.of(OPEN, HIDDEN), stats.getExpandWildcards());
        assertEquals(STATS_DEFAULTS.ignoreUnavailable(), stats.ignoreUnavailable());
        assertEquals(STATS_DEFAULTS.forbidClosedIndices(), stats.forbidClosedIndices());
    }

    public void testStatsExpandNothingForClosedOnly() {
        IndicesOptions requested = IndicesOptions.fromParameters("closed", null, null, null, IndicesOptions.lenientExpandOpen());
        assertTrue(TransportCatShardsAction.statsIndicesOptions(requested, STATS_DEFAULTS).getExpandWildcards().isEmpty());
    }
}
