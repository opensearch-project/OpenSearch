/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.autoforcemerge;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Set;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ForceMergeManagerSettingsTests extends OpenSearchTestCase {

    private ClusterSettings clusterSettings;
    private Settings settings;
    private ForceMergeManagerSettings forceMergeManagerSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder().build();
        clusterSettings = new ClusterSettings(
            settings,
            Set.of(
                ForceMergeManagerSettings.AUTO_FORCE_MERGE_SETTING,
                ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL,
                ForceMergeManagerSettings.TRANSLOG_AGE_AUTO_FORCE_MERGE,
                ForceMergeManagerSettings.SEGMENT_COUNT_FOR_AUTO_FORCE_MERGE,
                ForceMergeManagerSettings.MERGE_DELAY_BETWEEN_SHARDS_FOR_AUTO_FORCE_MERGE,
                ForceMergeManagerSettings.CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE,
                ForceMergeManagerSettings.DISK_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE,
                ForceMergeManagerSettings.JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE,
                ForceMergeManagerSettings.CONCURRENCY_MULTIPLIER
            )
        );
        Consumer<TimeValue> modifySchedulerInterval = new Consumer<>() {
            private TimeValue schedulerInterval;

            @Override
            public void accept(TimeValue timeValue) {
                this.schedulerInterval = timeValue;
            }
        };
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(clusterSettings);
        when(clusterService.getSettings()).thenReturn(settings);
        forceMergeManagerSettings = new ForceMergeManagerSettings(clusterService, modifySchedulerInterval);
    }

    public void testDefaultSettings() {
        assertEquals(false, forceMergeManagerSettings.isAutoForceMergeFeatureEnabled());
        assertEquals(forceMergeManagerSettings.getForcemergeDelay(), TimeValue.timeValueSeconds(15));
        assertEquals(forceMergeManagerSettings.getSchedulerInterval(), TimeValue.timeValueMinutes(30));
        assertEquals(2, (int) forceMergeManagerSettings.getConcurrencyMultiplier());
        assertEquals(1, (int) forceMergeManagerSettings.getSegmentCount());
        assertEquals(75.0, forceMergeManagerSettings.getCpuThreshold(), 0.0);
        assertEquals(75.0, forceMergeManagerSettings.getJvmThreshold(), 0.0);
        assertEquals(85.0, forceMergeManagerSettings.getDiskThreshold(), 0.0);
    }

    public void testDynamicSettingsUpdate() {
        Settings newSettings = Settings.builder()
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SETTING.getKey(), false)
            .put(ForceMergeManagerSettings.SEGMENT_COUNT_FOR_AUTO_FORCE_MERGE.getKey(), 30)
            .build();

        clusterSettings.applySettings(newSettings);

        assertEquals(false, forceMergeManagerSettings.isAutoForceMergeFeatureEnabled());
        assertEquals(30, (int) forceMergeManagerSettings.getSegmentCount());
    }

    public void testInvalidSettings() {
        expectThrows(IllegalArgumentException.class, () -> {
            Settings invalidSettings = Settings.builder()
                .put(ForceMergeManagerSettings.SEGMENT_COUNT_FOR_AUTO_FORCE_MERGE.getKey(), -1)
                .build();
            clusterSettings.applySettings(invalidSettings);
        });
        expectThrows(IllegalArgumentException.class, () -> {
            Settings invalidSettings = Settings.builder()
                .put(ForceMergeManagerSettings.CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 101.0)
                .build();
            clusterSettings.applySettings(invalidSettings);
        });
    }

    public void testTimeValueSettings() {
        Settings newSettings = Settings.builder()
            .put(ForceMergeManagerSettings.AUTO_FORCE_MERGE_SCHEDULER_INTERVAL.getKey(), "10m")
            .put(ForceMergeManagerSettings.TRANSLOG_AGE_AUTO_FORCE_MERGE.getKey(), "10m")
            .put(ForceMergeManagerSettings.MERGE_DELAY_BETWEEN_SHARDS_FOR_AUTO_FORCE_MERGE.getKey(), "20s")
            .build();

        clusterSettings.applySettings(newSettings);

        assertEquals(forceMergeManagerSettings.getSchedulerInterval(), TimeValue.timeValueMinutes(10));
        assertEquals(forceMergeManagerSettings.getTranslogAge(), TimeValue.timeValueMinutes(10));
        assertEquals(forceMergeManagerSettings.getForcemergeDelay(), TimeValue.timeValueSeconds(20));
    }

    public void testThreadSettings() {
        Settings newSettings = Settings.builder().put(ForceMergeManagerSettings.CONCURRENCY_MULTIPLIER.getKey(), 5).build();

        clusterSettings.applySettings(newSettings);

        assertEquals(5, (int) forceMergeManagerSettings.getConcurrencyMultiplier());
    }

    public void testThresholdSettings() {
        Settings newSettings = Settings.builder()
            .put(ForceMergeManagerSettings.CPU_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 60.0)
            .put(ForceMergeManagerSettings.DISK_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 70.0)
            .put(ForceMergeManagerSettings.JVM_THRESHOLD_PERCENTAGE_FOR_AUTO_FORCE_MERGE.getKey(), 70.0)
            .build();

        clusterSettings.applySettings(newSettings);
        assertEquals(60.0, forceMergeManagerSettings.getCpuThreshold(), 0.0);
        assertEquals(70.0, forceMergeManagerSettings.getDiskThreshold(), 0.0);
        assertEquals(70.0, forceMergeManagerSettings.getJvmThreshold(), 0.0);
    }

}
