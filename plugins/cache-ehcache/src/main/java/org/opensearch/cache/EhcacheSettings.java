/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cache;

import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeValue;

/**
 * Settings related to ehcache.
 */
public class EhcacheSettings {

    static final String SETTING_PREFIX = "cache.disk.ehcache";

    /**
     * Ehcache disk write minimum threads for its pool
     */
    public static final Setting<Integer> DISK_WRITE_MINIMUM_THREADS = Setting.intSetting(SETTING_PREFIX + "min_threads", 2, 1, 5);

    /**
     *  Ehcache disk write maximum threads for its pool
     */
    public static final Setting<Integer> DISK_WRITE_MAXIMUM_THREADS = Setting.intSetting(SETTING_PREFIX + ".max_threads", 2, 1, 20);

    /**
     *  Not be to confused with number of disk segments, this is different. Defines
     *  distinct write queues created for disk store where a group of segments share a write queue. This is
     *  implemented with ehcache using a partitioned thread pool exectutor By default all segments share a single write
     *  queue ie write concurrency is 1. Check OffHeapDiskStoreConfiguration and DiskWriteThreadPool.
     *
     *  Default is 1 within ehcache.
     */
    public static final Setting<Integer> DISK_WRITE_CONCURRENCY = Setting.intSetting(SETTING_PREFIX + ".concurrency", 1, 1, 3);

    /**
     * Defines how many segments the disk cache is separated into. Higher number achieves greater concurrency but
     * will hold that many file pointers. Default is 16.
     *
     * Default value is 16 within Ehcache.
     */
    public static final Setting<Integer> DISK_SEGMENTS = Setting.intSetting(SETTING_PREFIX + ".segments", 16, 1, 32);

    /**
     * Defines whether to use an in-memory keystore to check for probable presence of keys before having to go to disk.
     */
    public static final Setting<Boolean> USE_RBM_KEYSTORE = Setting.boolSetting(SETTING_PREFIX + ".use_keystore", true);

    /**
     * Defines the max size of the RBM keystore if used (as a percentage of heap memory)
     */
    public static final Setting<ByteSizeValue> RBM_KEYSTORE_SIZE = Setting.memorySizeSetting(
        SETTING_PREFIX + ".keystore_size",
        "0.05%",
        Setting.Property.Dynamic
    );

    /**
     * Default constructor. Added to fix javadocs.
     */
    public EhcacheSettings() {}
}
