/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Setting;

/**
 * Keys of the workload group {@code queue} object, a nested {@link org.opensearch.common.settings.Settings} bag alongside
 * {@code throttling} that is reserved for request queueing. This version does not queue requests: it rejects a
 * {@code queue} object in create and update requests, and only carries one it reads from cluster state (parse, persist,
 * emit, forward), unvalidated. Workload group XContent writes {@code size_per_bucket} as a JSON number when its stored
 * value is an integer; any other value, and every other stored key, is written verbatim as a string.
 */
@ExperimentalApi
public class WorkloadGroupQueueSettings {

    /** Default of {@link #SIZE_PER_BUCKET}. */
    public static final int DEFAULT_SIZE_PER_BUCKET = 0;

    /** Reserved for request queueing, which does not read it in this version. */
    public static final int MAX_GROUP_QUEUE_DEPTH = 1_000;

    /** Upper bound of {@link #SIZE_PER_BUCKET}. */
    public static final int MAX_SIZE_PER_BUCKET = MAX_GROUP_QUEUE_DEPTH;

    /** The {@code queue.size_per_bucket} key, an integer. Nothing reads its value in this version. */
    public static final Setting<Integer> SIZE_PER_BUCKET = Setting.intSetting(
        "size_per_bucket",
        DEFAULT_SIZE_PER_BUCKET,
        0,
        MAX_SIZE_PER_BUCKET
    );

    private WorkloadGroupQueueSettings() {
        throw new UnsupportedOperationException("Utility class");
    }

}
