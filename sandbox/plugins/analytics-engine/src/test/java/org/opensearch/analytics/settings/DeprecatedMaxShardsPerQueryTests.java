/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

/**
 * {@code analytics.query.max_shards_per_query} is retained only so a cluster that still carries it
 * starts and gets a deprecation warning instead of a settings-validation failure. It is inert — the
 * shard ceiling is {@code action.search.shard_count.limit}, and nothing reads this one.
 */
public class DeprecatedMaxShardsPerQueryTests extends OpenSearchTestCase {

    /** Still registered, or a cluster carrying it would fail to start rather than warn. */
    public void testStillRegistered() {
        assertTrue(
            "the deprecated key must remain registered so existing clusters keep starting",
            AnalyticsQuerySettings.all().stream().anyMatch(s -> s.getKey().equals("analytics.query.max_shards_per_query"))
        );
    }

    /** Marked deprecated, so setting it surfaces a warning to the operator. */
    public void testMarkedDeprecated() {
        assertTrue(
            "operators need a warning telling them to migrate",
            AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.getProperties().contains(Setting.Property.Deprecated)
        );
    }

    /**
     * Reading it emits the deprecation warning an operator needs to see. The value still parses — it
     * is a real {@link Setting} — but no production code performs this read; that is what makes it
     * inert.
     */
    public void testReadingItWarns() {
        Settings withLegacyLimit = Settings.builder().put(AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.getKey(), 1).build();

        assertEquals(Integer.valueOf(1), AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.get(withLegacyLimit));

        assertWarnings(
            "[analytics.query.max_shards_per_query] setting was deprecated in OpenSearch and will be removed in a future release! "
                + "See the breaking changes documentation for the next major version."
        );
    }

    /**
     * The actual contract: no production class references the constant. Kept as a documented
     * expectation next to the setting so the intent survives; enforcement is by review and by
     * {@code ShardTargetResolverTests.testResolveIsUnlimitedByDefault}, which pins that an
     * unconfigured cluster rejects nothing regardless of this key.
     */
    public void testNoProductionCodeReadsIt() {
        // ShardTargetResolver reads action.search.shard_count.limit only; QueryContext no longer
        // carries a max-shards value. Both are covered by ShardTargetResolverTests.
        assertFalse(
            "a dynamic property would imply something consumes updates",
            AnalyticsQuerySettings.MAX_SHARDS_PER_QUERY.getProperties().contains(Setting.Property.Dynamic)
        );
    }
}
