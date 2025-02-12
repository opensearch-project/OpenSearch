/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import static org.opensearch.cluster.coordination.Coordinator.PUBLISH_TIMEOUT_SETTING;
import static org.opensearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.opensearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.opensearch.cluster.coordination.LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING;
import static org.opensearch.cluster.coordination.LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING;
import static org.opensearch.common.unit.TimeValue.timeValueSeconds;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class CoordinationCheckerSettingsTests extends OpenSearchSingleNodeTestCase {
    public void testFollowerCheckTimeoutValueUpdate() {
        Setting<TimeValue> setting1 = FOLLOWER_CHECK_TIMEOUT_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "60s").build();
        try {
            ClusterUpdateSettingsResponse response = client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(timeSettings1)
                .execute()
                .actionGet();

            assertAcked(response);
            assertEquals(timeValueSeconds(60), setting1.get(response.getPersistentSettings()));
        } finally {
            // cleanup
            timeSettings1 = Settings.builder().putNull(setting1.getKey()).build();
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
        }
    }

    public void testFollowerCheckTimeoutMaxValue() {
        Setting<TimeValue> setting1 = FOLLOWER_CHECK_TIMEOUT_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "151s").build();

        assertThrows(
            "failed to parse value [151s] for setting [" + setting1.getKey() + "], must be <= [150000ms]",
            IllegalArgumentException.class,
            () -> {
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
            }
        );
    }

    public void testFollowerCheckTimeoutMinValue() {
        Setting<TimeValue> setting1 = FOLLOWER_CHECK_TIMEOUT_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "0s").build();

        assertThrows(
            "failed to parse value [0s] for setting [" + setting1.getKey() + "], must be >= [1ms]",
            IllegalArgumentException.class,
            () -> {
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
            }
        );
    }

    public void testFollowerCheckIntervalValueUpdate() {
        Setting<TimeValue> setting1 = FOLLOWER_CHECK_INTERVAL_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "10s").build();
        try {
            ClusterUpdateSettingsResponse response = client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(timeSettings1)
                .execute()
                .actionGet();
            assertAcked(response);
            assertEquals(timeValueSeconds(10), setting1.get(response.getPersistentSettings()));
        } finally {
            // cleanup
            timeSettings1 = Settings.builder().putNull(setting1.getKey()).build();
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
        }
    }

    public void testFollowerCheckIntervalMinValue() {
        Setting<TimeValue> setting1 = FOLLOWER_CHECK_INTERVAL_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "10ms").build();

        assertThrows(
            "failed to parse value [10ms] for setting [" + setting1.getKey() + "], must be >= [100ms]",
            IllegalArgumentException.class,
            () -> {
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
            }
        );
    }

    public void testLeaderCheckTimeoutValueUpdate() {
        Setting<TimeValue> setting1 = LEADER_CHECK_TIMEOUT_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "60s").build();
        try {
            ClusterUpdateSettingsResponse response = client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(timeSettings1)
                .execute()
                .actionGet();
            assertAcked(response);
            assertEquals(timeValueSeconds(60), setting1.get(response.getPersistentSettings()));
        } finally {
            // cleanup
            timeSettings1 = Settings.builder().putNull(setting1.getKey()).build();
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
        }
    }

    public void testLeaderCheckTimeoutMaxValue() {
        Setting<TimeValue> setting1 = LEADER_CHECK_TIMEOUT_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "61s").build();

        assertThrows(
            "failed to parse value [61s] for setting [" + setting1.getKey() + "], must be <= [60000ms]",
            IllegalArgumentException.class,
            () -> {
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
            }
        );
    }

    public void testLeaderCheckTimeoutMinValue() {
        Setting<TimeValue> setting1 = LEADER_CHECK_TIMEOUT_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "0s").build();

        assertThrows(
            "failed to parse value [0s] for setting [" + setting1.getKey() + "], must be >= [1ms]",
            IllegalArgumentException.class,
            () -> {
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
            }
        );
    }

    public void testClusterPublishTimeoutValueUpdate() {
        Setting<TimeValue> setting1 = PUBLISH_TIMEOUT_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "60s").build();
        try {
            ClusterUpdateSettingsResponse response = client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(timeSettings1)
                .execute()
                .actionGet();
            assertAcked(response);
            assertEquals(timeValueSeconds(60), setting1.get(response.getPersistentSettings()));
        } finally {
            // cleanup
            timeSettings1 = Settings.builder().putNull(setting1.getKey()).build();
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
        }
    }

    public void testClusterPublishTimeoutMinValue() {
        Setting<TimeValue> setting1 = PUBLISH_TIMEOUT_SETTING;
        Settings timeSettings1 = Settings.builder().put(setting1.getKey(), "0s").build();

        assertThrows(
            "failed to parse value [0s] for setting [" + setting1.getKey() + "], must be >= [1ms]",
            IllegalArgumentException.class,
            () -> {
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(timeSettings1).execute().actionGet();
            }
        );
    }

    public void testLagDetectorTimeoutUpdate() {
        Setting<TimeValue> setting1 = CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING;
        Settings lagDetectorTimeout = Settings.builder().put(setting1.getKey(), "30s").build();
        try {
            ClusterUpdateSettingsResponse response = client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(lagDetectorTimeout)
                .execute()
                .actionGet();

            assertAcked(response);
            assertEquals(timeValueSeconds(30), setting1.get(response.getPersistentSettings()));
        } finally {
            // cleanup
            lagDetectorTimeout = Settings.builder().putNull(setting1.getKey()).build();
            client().admin().cluster().prepareUpdateSettings().setPersistentSettings(lagDetectorTimeout).execute().actionGet();
        }
    }

    public void testLagDetectorTimeoutMinValue() {
        Setting<TimeValue> setting1 = CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING;
        Settings lagDetectorTimeout = Settings.builder().put(setting1.getKey(), "0s").build();

        assertThrows(
            "failed to parse value [0s] for setting [" + setting1.getKey() + "], must be >= [1ms]",
            IllegalArgumentException.class,
            () -> {
                client().admin().cluster().prepareUpdateSettings().setPersistentSettings(lagDetectorTimeout).execute().actionGet();
            }
        );
    }

}
