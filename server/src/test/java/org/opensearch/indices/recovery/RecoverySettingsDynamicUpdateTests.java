/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.recovery;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

public class RecoverySettingsDynamicUpdateTests extends OpenSearchTestCase {
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, clusterSettings);

    public void testZeroBytesPerSecondIsNoRateLimit() {
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), 0).build()
        );
        assertNull(recoverySettings.recoveryRateLimiter());
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_REPLICATION_MAX_BYTES_PER_SEC_SETTING.getKey(), 0).build()
        );
        assertNull(recoverySettings.replicationRateLimiter());
    }

    public void testSetReplicationMaxBytesPerSec() {
        assertEquals(40, (int) recoverySettings.replicationRateLimiter().getMBPerSec());
        clusterSettings.applySettings(
            Settings.builder()
                .put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), new ByteSizeValue(60, ByteSizeUnit.MB))
                .build()
        );
        assertEquals(60, (int) recoverySettings.replicationRateLimiter().getMBPerSec());
        clusterSettings.applySettings(
            Settings.builder()
                .put(RecoverySettings.INDICES_REPLICATION_MAX_BYTES_PER_SEC_SETTING.getKey(), new ByteSizeValue(80, ByteSizeUnit.MB))
                .build()
        );
        assertEquals(80, (int) recoverySettings.replicationRateLimiter().getMBPerSec());
    }

    public void testRetryDelayStateSync() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.getKey(), duration, timeUnit).build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.retryDelayStateSync());
    }

    public void testRetryDelayNetwork() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), duration, timeUnit).build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.retryDelayNetwork());
    }

    public void testActivityTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.getKey(), duration, timeUnit).build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.activityTimeout());
    }

    public void testInternalActionTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), duration, timeUnit).build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.internalActionTimeout());
    }

    public void testInternalLongActionTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder()
                .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.getKey(), duration, timeUnit)
                .build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.internalActionLongTimeout());
    }

    public void testChunkSize() {
        ByteSizeValue chunkSize = new ByteSizeValue(between(1, 1000), ByteSizeUnit.BYTES);
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_CHUNK_SIZE_SETTING.getKey(), chunkSize).build()
        );
        assertEquals(chunkSize, recoverySettings.getChunkSize());
    }

    public void testInternalActionRetryTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder()
                .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_RETRY_TIMEOUT_SETTING.getKey(), duration, timeUnit)
                .build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.internalActionRetryTimeout());
    }
}
