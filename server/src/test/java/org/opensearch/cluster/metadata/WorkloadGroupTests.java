/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.Version;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;
import org.opensearch.wlm.MutableWorkloadGroupFragment;
import org.opensearch.wlm.MutableWorkloadGroupFragment.ResiliencyMode;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.WorkloadGroupThrottleSettings;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class WorkloadGroupTests extends AbstractSerializingTestCase<WorkloadGroup> {

    private static final List<ResiliencyMode> allowedModes = List.of(ResiliencyMode.SOFT, ResiliencyMode.ENFORCED, ResiliencyMode.MONITOR);
    public static final Settings TEST_WLM_SEARCH_SETTINGS = Settings.builder().put("search.default_search_timeout", "30s").build();

    static WorkloadGroup createRandomWorkloadGroup(String _id) {
        String name = randomAlphaOfLength(10);
        Map<ResourceType, Double> resourceLimit = new HashMap<>();
        resourceLimit.put(ResourceType.MEMORY, randomDoubleBetween(0.0, 0.80, false));
        // Generate a valid throttling config: either disabled (empty), or enabled with a required attribute plus
        // a positive node_limit (so the effective ceiling is >= 1).
        Settings.Builder throttling = Settings.builder();
        if (randomBoolean()) {
            throttling.put("attribute", randomFrom("group", "username", "role"));
            throttling.put("node_limit", randomIntBetween(1, 100));
        }
        return new WorkloadGroup(
            name,
            _id,
            new MutableWorkloadGroupFragment(randomMode(), resourceLimit, Settings.EMPTY, throttling.build()),
            Instant.now().getMillis()
        );
    }

    private static ResiliencyMode randomMode() {
        return allowedModes.get(randomIntBetween(0, allowedModes.size() - 1));
    }

    /**
     * Parses to a new instance using the provided {@link XContentParser}
     *
     * @param parser the XContentParser
     */
    @Override
    protected WorkloadGroup doParseInstance(XContentParser parser) throws IOException {
        return WorkloadGroup.fromXContent(parser);
    }

    /**
     * Returns a {@link Writeable.Reader} that can be used to de-serialize the instance
     */
    @Override
    protected Writeable.Reader<WorkloadGroup> instanceReader() {
        return WorkloadGroup::new;
    }

    /**
     * Creates a random test instance to use in the tests. This method will be
     * called multiple times during test execution and should return a different
     * random instance each time it is called.
     */
    @Override
    protected WorkloadGroup createTestInstance() {
        return createRandomWorkloadGroup("1232sfraeradf_");
    }

    public void testNullName() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup(
                null,
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testNullId() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup(
                "Dummy",
                null,
                new MutableWorkloadGroupFragment(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testNullResourceLimits() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup("analytics", "_id", new MutableWorkloadGroupFragment(randomMode(), null), Instant.now().getMillis())
        );
    }

    public void testEmptyResourceLimits() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Collections.emptyMap()),
                Instant.now().getMillis()
            )
        );
    }

    public void testIllegalWorkloadGroupMode() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(null, Map.of(ResourceType.MEMORY, 0.4)),
                Instant.now().getMillis()
            )
        );
    }

    public void testWorkloadGroupInitiation() {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "analytics",
            new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, 0.4), TEST_WLM_SEARCH_SETTINGS)
        );
        assertNotNull(workloadGroup.getName());
        assertNotNull(workloadGroup.get_id());
        assertNotNull(workloadGroup.getResourceLimits());
        assertFalse(workloadGroup.getResourceLimits().isEmpty());
        assertEquals(1, workloadGroup.getResourceLimits().size());
        assertTrue(allowedModes.contains(workloadGroup.getResiliencyMode()));
        assertTrue(workloadGroup.getUpdatedAtInMillis() != 0);
        assertNotNull(workloadGroup.getSettings());
        assertEquals(TEST_WLM_SEARCH_SETTINGS, workloadGroup.getSettings());
    }

    public void testIllegalWorkloadGroupName() {
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup("a".repeat(51), "_id", new MutableWorkloadGroupFragment(), Instant.now().getMillis())
        );
        assertThrows(
            NullPointerException.class,
            () -> new WorkloadGroup("", "_id", new MutableWorkloadGroupFragment(), Instant.now().getMillis())
        );

    }

    public void testInvalidResourceLimitWhenInvalidSystemResourceValueIsGiven() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(1.1, 1.8, false))),
                Instant.now().getMillis()
            )
        );
    }

    public void testValidWorkloadGroup() {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "analytics",
            "_id",
            new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
            Instant.ofEpochMilli(1717187289).getMillis()
        );

        assertNotNull(workloadGroup.getName());
        assertEquals("analytics", workloadGroup.getName());
        assertNotNull(workloadGroup.getResourceLimits());
        assertFalse(workloadGroup.getResourceLimits().isEmpty());
        assertEquals(1, workloadGroup.getResourceLimits().size());
        assertTrue(allowedModes.contains(workloadGroup.getResiliencyMode()));
        assertEquals(1717187289, workloadGroup.getUpdatedAtInMillis());
    }

    public void testUpdatedAtAllowsCurrentTimestamp() {
        long currentTimestamp = Instant.now().getMillis();
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "analytics",
            "_id",
            new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
            currentTimestamp
        );

        assertEquals(currentTimestamp, workloadGroup.getUpdatedAtInMillis());
    }

    public void testUpdatedAtAllowsJitterAroundNow() {
        long now = Instant.now().getMillis();
        long[] timestamps = new long[] { Math.max(0L, now - 20L), now + 20L };

        for (long timestamp : timestamps) {
            WorkloadGroup workloadGroup = new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
                timestamp
            );
            assertEquals(timestamp, workloadGroup.getUpdatedAtInMillis());
        }
    }

    public void testUpdatedAtRejectsNegativeTimestamp() {
        assertThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "analytics",
                "_id",
                new MutableWorkloadGroupFragment(randomMode(), Map.of(ResourceType.MEMORY, randomDoubleBetween(0.01, 0.8, false))),
                -1L
            )
        );
    }

    public void testToXContent() throws IOException {
        long currentTimeInMillis = Instant.now().getMillis();
        String workloadGroupId = UUIDs.randomBase64UUID();
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "TestWorkloadGroup",
            workloadGroupId,
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.CPU, 0.30, ResourceType.MEMORY, 0.40),
                TEST_WLM_SEARCH_SETTINGS
            ),
            currentTimeInMillis
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        workloadGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String expected = String.format(
            Locale.ROOT,
            "{\"_id\":\"%s\",\"name\":\"TestWorkloadGroup\",\"resiliency_mode\":\"enforced\","
                + "\"resource_limits\":{\"cpu\":0.3,\"memory\":0.4},"
                + "\"settings\":{\"search.default_search_timeout\":\"30s\"},"
                + "\"updated_at\":%d}",
            workloadGroupId,
            currentTimeInMillis
        );
        assertEquals(expected, builder.toString());
    }

    public void testLegacySearchSettingsFieldRejected() throws IOException {
        String json = "{\"_id\":\"test_id\",\"name\":\"test\",\"resiliency_mode\":\"enforced\","
            + "\"resource_limits\":{\"memory\":0.5},"
            + "\"search_settings\":{\"timeout\":\"30s\"},"
            + "\"updated_at\":1720047207}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> WorkloadGroup.fromXContent(parser));
        assertTrue(exception.getMessage().contains("search_settings"));
    }

    public void testUpdateWithEmptySettingsClearsExisting() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").build()
            ),
            System.currentTimeMillis()
        );

        // Empty settings should clear all search settings
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(null, Map.of(), Settings.EMPTY);

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        // All settings should be cleared
        assertTrue(updated.getSettings().isEmpty());
    }

    public void testUpdateMergesSettings() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").put("search.max_concurrent_shard_requests", "5").build()
            ),
            System.currentTimeMillis()
        );

        // Update only timeout — max_concurrent_shard_requests should persist
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(
            null,
            Map.of(),
            Settings.builder().put("search.default_search_timeout", "1m").build()
        );

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertEquals("1m", updated.getSettings().get("search.default_search_timeout"));
        assertEquals("5", updated.getSettings().get("search.max_concurrent_shard_requests"));
    }

    public void testUpdateWithNullValueRemovesSetting() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").put("search.max_concurrent_shard_requests", "5").build()
            ),
            System.currentTimeMillis()
        );

        // Send null for timeout — should remove it, keep max_concurrent
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(
            null,
            Map.of(),
            Settings.builder().putNull("search.default_search_timeout").build()
        );

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertNull(updated.getSettings().get("search.default_search_timeout"));
        assertEquals("5", updated.getSettings().get("search.max_concurrent_shard_requests"));
    }

    public void testUpdateWithNullFragmentSettingsKeepsExisting() throws IOException {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").build()
            ),
            System.currentTimeMillis()
        );

        // Parse an update request that doesn't include "settings" key
        String json = "{\"resiliency_mode\":\"soft\",\"resource_limits\":{\"memory\":0.6}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        WorkloadGroup.Builder builder = WorkloadGroup.Builder.fromXContent(parser);
        MutableWorkloadGroupFragment updateFragment = builder.getMutableWorkloadGroupFragment();

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        // Settings should be preserved
        assertEquals("30s", updated.getSettings().get("search.default_search_timeout"));
        assertEquals(ResiliencyMode.SOFT, updated.getResiliencyMode());
    }

    public void testUpdateOverrideRequestValuesPersistsThroughMerge() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.builder().put("search.default_search_timeout", "30s").put("override_request_values", "true").build()
            ),
            System.currentTimeMillis()
        );

        // Update only timeout — override_request_values should persist as "true"
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(
            null,
            Map.of(),
            Settings.builder().put("search.default_search_timeout", "1m").build()
        );

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertEquals("1m", updated.getSettings().get("search.default_search_timeout"));
        assertEquals("true", updated.getSettings().get("override_request_values"));
    }

    public void testToXContentOmitsUnsetThrottling() throws IOException {
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.MEMORY, 0.5), Settings.EMPTY),
            System.currentTimeMillis()
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        workloadGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertFalse(builder.toString().contains("throttling"));
    }

    public void testToXContentEmitsThrottling() throws IOException {
        long currentTimeInMillis = Instant.now().getMillis();
        String workloadGroupId = UUIDs.randomBase64UUID();
        Settings throttling = Settings.builder().put("attribute", "username").put("node_limit", 10).build();
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "TestWorkloadGroup",
            workloadGroupId,
            new MutableWorkloadGroupFragment(ResiliencyMode.ENFORCED, Map.of(ResourceType.CPU, 0.30), Settings.EMPTY, throttling),
            currentTimeInMillis
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        workloadGroup.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String expected = String.format(
            Locale.ROOT,
            "{\"_id\":\"%s\",\"name\":\"TestWorkloadGroup\",\"resiliency_mode\":\"enforced\","
                + "\"resource_limits\":{\"cpu\":0.3},"
                + "\"settings\":{},"
                + "\"throttling\":{\"attribute\":\"username\",\"node_limit\":10},"
                + "\"updated_at\":%d}",
            workloadGroupId,
            currentTimeInMillis
        );
        assertEquals(expected, builder.toString());
    }

    public void testNegativeThrottleLimitRejected() {
        // -1 is the internal "unset" sentinel and, like any negative value, is not user-settable.
        for (int badLimit : new int[] { -1, -2 }) {
            IllegalArgumentException exception = expectThrows(
                IllegalArgumentException.class,
                () -> new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("node_limit", badLimit).build()
                )
            );
            assertTrue(exception.getMessage().contains("node_limit must be non-negative"));
        }
    }

    public void testThrottleLimitExceedingMaxRejected() {
        // Integer.MAX_VALUE + 1: a well-formed non-negative integer, but too large for the int-backed setting. The error
        // must call out the overflow rather than falsely claiming it is "not an integer".
        String tooLarge = Long.toString((long) Integer.MAX_VALUE + 1);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "username").put("node_limit", tooLarge).build()
            )
        );
        assertTrue(exception.getMessage().contains("node_limit must not exceed " + Integer.MAX_VALUE));
        assertTrue(exception.getMessage().contains(tooLarge));
    }

    public void testThrottleLimitAtMaxAccepted() {
        // Integer.MAX_VALUE is the largest limit the int-backed setting can hold and must be accepted.
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "username").put("node_limit", Integer.MAX_VALUE).build()
            ),
            System.currentTimeMillis()
        );
        Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
        assertEquals(Integer.valueOf(Integer.MAX_VALUE), WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling));
    }

    public void testNonNumericThrottleLimitRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "username").put("node_limit", "not_a_number").build()
            )
        );
        assertTrue(exception.getMessage().contains("node_limit must be an integer"));
    }

    public void testInvalidThrottleAttributeRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "index").put("node_limit", 5).build()
            )
        );
        assertTrue(exception.getMessage().contains("throttling.attribute must be one of"));
    }

    public void testUnknownThrottleKeyRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("bogus_limit", 5).build()
            )
        );
        assertTrue(exception.getMessage().contains("Unknown throttle setting"));
    }

    public void testZeroEffectiveCeilingRejected() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "test",
                "test_id",
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("attribute", "username").put("node_limit", 0).build()
                ),
                System.currentTimeMillis()
            )
        );
        assertTrue(exception.getMessage().contains("Effective throttle ceiling is 0"));
    }

    public void testAttributeWithoutLimitRejected() {
        // An attribute alone configures nothing, so the error must say a limit is missing rather than report a zero
        // ceiling (nothing was set, so nothing "would reject all requests"), and must never leak the -1 sentinel.
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "test",
                "test_id",
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("attribute", "username").build()
                ),
                System.currentTimeMillis()
            )
        );
        assertTrue(exception.getMessage().contains("throttling.node_limit is required when throttling.attribute is set"));
        assertFalse(exception.getMessage().contains("-1"));
        assertFalse(exception.getMessage().contains("ceiling"));
    }

    public void testLimitWithoutAttributeRejected() {
        // A throttle limit requires an attribute; a limit with no attribute is rejected.
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "test",
                "test_id",
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("node_limit", 5).build()
                ),
                System.currentTimeMillis()
            )
        );
        assertTrue(exception.getMessage().contains("throttling.attribute is required"));
    }

    public void testWholeGroupThrottleWithExplicitAttribute() {
        // attribute has no default; whole-group throttling must be requested explicitly with attribute=group.
        WorkloadGroup workloadGroup = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "group").put("node_limit", 5).build()
            ),
            System.currentTimeMillis()
        );
        Settings throttling = workloadGroup.getMutableWorkloadGroupFragment().getThrottling();
        assertEquals("group", WorkloadGroupThrottleSettings.ATTRIBUTE.get(throttling));
        assertEquals(Integer.valueOf(5), WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling));
    }

    public void testUpdateMergesThrottling() {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "username").put("node_limit", 10).build()
            ),
            System.currentTimeMillis()
        );

        // Update only node_limit — the absent attribute key should keep its existing value
        MutableWorkloadGroupFragment updateFragment = new MutableWorkloadGroupFragment(
            null,
            Map.of(),
            Settings.EMPTY,
            Settings.builder().put("node_limit", 50).build()
        );

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        Settings throttling = updated.getMutableWorkloadGroupFragment().getThrottling();
        assertEquals("username", WorkloadGroupThrottleSettings.ATTRIBUTE.get(throttling));
        assertEquals(Integer.valueOf(50), WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling));
    }

    public void testUpdateWithNullClearsThrottleKeys() throws IOException {
        // Clearing every throttle key individually is equivalent to disabling throttling: the merge consumes each null
        // and the bag collapses to empty. Clearing only node_limit is rejected instead, because that would leave an
        // attribute with no limit, which configures nothing; "throttling": null is the way to disable one key at a time.
        String json = "{\"resource_limits\":{\"memory\":0.5},\"throttling\":{\"attribute\":null,\"node_limit\":null}}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        MutableWorkloadGroupFragment clearAll = WorkloadGroup.Builder.fromXContent(parser).getMutableWorkloadGroupFragment();

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(throttledGroup(), clearAll);
        assertTrue(updated.getMutableWorkloadGroupFragment().getThrottling().isEmpty());

        String clearLimitOnly = "{\"resource_limits\":{\"memory\":0.5},\"throttling\":{\"node_limit\":null}}";
        XContentParser limitParser = createParser(JsonXContent.jsonXContent, clearLimitOnly);
        MutableWorkloadGroupFragment clearLimit = WorkloadGroup.Builder.fromXContent(limitParser).getMutableWorkloadGroupFragment();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> WorkloadGroup.updateExistingWorkloadGroup(throttledGroup(), clearLimit)
        );
        assertTrue(exception.getMessage().contains("throttling.node_limit is required when throttling.attribute is set"));
    }

    public void testUpdateFromPreThrottlingPeerPreservesThrottling() throws IOException {
        // A pre-3.9 node has no throttling field, so it writes none. Decoding "absent" as an empty bag would make
        // mergeSettings treat it as the explicit "clear all" gesture and silently delete the group's throttling on an
        // update that never mentioned throttling.
        MutableWorkloadGroupFragment update = new MutableWorkloadGroupFragment(
            ResiliencyMode.SOFT,
            Map.of(),
            Settings.EMPTY,
            Settings.EMPTY
        );
        MutableWorkloadGroupFragment asSeenByCurrentNode = copyWriteable(
            update,
            new NamedWriteableRegistry(Collections.emptyList()),
            MutableWorkloadGroupFragment::new,
            Version.V_3_8_0
        );

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(throttledGroup(), asSeenByCurrentNode);
        Settings throttling = updated.getMutableWorkloadGroupFragment().getThrottling();
        assertEquals("username", WorkloadGroupThrottleSettings.ATTRIBUTE.get(throttling));
        assertEquals(Integer.valueOf(10), WorkloadGroupThrottleSettings.NODE_LIMIT.get(throttling));
        assertEquals(ResiliencyMode.SOFT, updated.getResiliencyMode());
    }

    public void testThrottlingIsDroppedWhenWrittenToPreThrottlingPeer() throws IOException {
        // This wire gate is why throttling must also be validated on the node that ACCEPTS a create/update, not only on the
        // elected cluster-manager. When the coordinator is not the manager the request is serialized at the manager's
        // version, so forwarding to a pre-3.9 manager omits the throttling bag entirely; that manager runs older plugin code
        // with no such validation, persists a group without throttling and answers 200. The truncated stream stays
        // well-formed, which is precisely why the loss is silent rather than an error.
        MutableWorkloadGroupFragment withThrottling = new MutableWorkloadGroupFragment(
            ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 0.5),
            Settings.EMPTY,
            Settings.builder().put("attribute", "group").put("node_limit", 7).build()
        );

        MutableWorkloadGroupFragment asSeenByOldPeer = copyWriteable(
            withThrottling,
            new NamedWriteableRegistry(Collections.emptyList()),
            MutableWorkloadGroupFragment::new,
            Version.V_3_8_0
        );
        assertNull("a pre-3.9 peer must not receive a throttling bag at all", asSeenByOldPeer.getThrottling());

        MutableWorkloadGroupFragment asSeenByCurrentPeer = copyWriteable(
            withThrottling,
            new NamedWriteableRegistry(Collections.emptyList()),
            MutableWorkloadGroupFragment::new,
            Version.V_3_9_0
        );
        assertEquals(Integer.valueOf(7), WorkloadGroupThrottleSettings.NODE_LIMIT.get(asSeenByCurrentPeer.getThrottling()));
    }

    public void testDeserializationAcceptsThrottlingThisNodeConsidersInvalid() throws IOException {
        // Cluster state published by a newer node may use throttling rules this node does not know (e.g. a second limit
        // key, making node_limit optional). Rejecting it here would wedge the node out of the cluster instead of
        // failing one API call, so the deserialization path must accept it.
        WorkloadGroup valid = throttledGroup();
        BytesStreamOutput out = new BytesStreamOutput();
        out.writeString(valid.getName());
        out.writeString(valid.get_id());
        new MutableWorkloadGroupFragment(
            ResiliencyMode.ENFORCED,
            Map.of(ResourceType.MEMORY, 0.5),
            Settings.EMPTY,
            // attribute with no limit: rejected on the API path, must be tolerated on the wire
            Settings.builder().put("attribute", "username").build()
        ).writeTo(out);
        out.writeLong(System.currentTimeMillis());

        StreamInput in = out.bytes().streamInput();
        WorkloadGroup deserialized = new WorkloadGroup(in);
        assertEquals(
            "username",
            WorkloadGroupThrottleSettings.ATTRIBUTE.get(deserialized.getMutableWorkloadGroupFragment().getThrottling())
        );

        // The same config through the API path is still rejected.
        expectThrows(
            IllegalArgumentException.class,
            () -> new WorkloadGroup(
                "test",
                "test_id",
                new MutableWorkloadGroupFragment(
                    ResiliencyMode.ENFORCED,
                    Map.of(ResourceType.MEMORY, 0.5),
                    Settings.EMPTY,
                    Settings.builder().put("attribute", "username").build()
                ),
                System.currentTimeMillis()
            )
        );
    }

    private static WorkloadGroup throttledGroup() {
        return new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "username").put("node_limit", 10).build()
            ),
            System.currentTimeMillis()
        );
    }

    public void testUpdateWithNullThrottlingObjectDisables() throws IOException {
        WorkloadGroup existing = new WorkloadGroup(
            "test",
            "test_id",
            new MutableWorkloadGroupFragment(
                ResiliencyMode.ENFORCED,
                Map.of(ResourceType.MEMORY, 0.5),
                Settings.EMPTY,
                Settings.builder().put("attribute", "username").put("node_limit", 10).build()
            ),
            System.currentTimeMillis()
        );

        // "throttling": null disables throttling entirely
        String json = "{\"resource_limits\":{\"memory\":0.5},\"throttling\":null}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        MutableWorkloadGroupFragment updateFragment = WorkloadGroup.Builder.fromXContent(parser).getMutableWorkloadGroupFragment();

        WorkloadGroup updated = WorkloadGroup.updateExistingWorkloadGroup(existing, updateFragment);
        assertTrue(updated.getMutableWorkloadGroupFragment().getThrottling().isEmpty());
    }

    public void testCreateDropsNullThrottleValues() throws IOException {
        // On create there is nothing to clear, so null-valued keys are dropped rather than persisted; an
        // all-null throttling object therefore collapses to empty (disabled) instead of hitting a ceiling error.
        WorkloadGroup allNull = parseCreate(
            "{\"resiliency_mode\":\"enforced\",\"resource_limits\":{\"memory\":0.5}," + "\"throttling\":{\"node_limit\":null}}"
        );
        Settings throttling = allNull.getMutableWorkloadGroupFragment().getThrottling();
        assertTrue(throttling.isEmpty());
        assertFalse(throttling.keySet().contains("node_limit")); // raw check: null-valued key was dropped, not persisted
    }

    private WorkloadGroup parseCreate(String json) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        return WorkloadGroup.Builder.fromXContent(parser).name("test")._id("test_id").updatedAt(System.currentTimeMillis()).build();
    }

    public void testSettingsNullFromXContentClearsSettings() throws IOException {
        // Simulate parsing {"settings": null} via XContent
        String json = "{\"_id\":\"test_id\",\"name\":\"test\",\"resiliency_mode\":\"enforced\","
            + "\"resource_limits\":{\"memory\":0.5},"
            + "\"settings\":null,"
            + "\"updated_at\":1720047207}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, json);
        WorkloadGroup.Builder builder = WorkloadGroup.Builder.fromXContent(parser);
        MutableWorkloadGroupFragment fragment = builder.getMutableWorkloadGroupFragment();
        // Settings should be empty (cleared)
        assertTrue(fragment.getSettings().isEmpty());
    }
}
