/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.opensearch.datafusion.DataFusionQueryJNI.cacheManagerUpdateSizeLimitForCacheType;
import static org.opensearch.datafusion.DataFusionQueryJNI.createCache;
import static org.opensearch.datafusion.DataFusionQueryJNI.createCustomCacheManager;
import static org.opensearch.datafusion.DataFusionQueryJNI.initCacheManagerConfig;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;

/**
 * Utility class for cache initialization and configuration.
 * Contains the CacheType enum and methods for creating cache configurations.
 */
public final class CacheUtils {
    private static final Logger logger = LogManager.getLogger(CacheUtils.class);

    // Private constructor to prevent instantiation
    private CacheUtils() {}

    /**
     * Cache type enumeration with associated settings.
     */
    public enum CacheType {
        METADATA(
            "METADATA",
            METADATA_CACHE_ENABLED,
            METADATA_CACHE_SIZE_LIMIT,
            METADATA_CACHE_EVICTION_TYPE
        );
        // STATS("STATS", STATS_CACHE_ENABLED, STATS_CACHE_SIZE_LIMIT, STATS_CACHE_EVICTION_TYPE);

        private final String cacheTypeName;
        private final Setting<Boolean> enabledSetting;
        private final Setting<ByteSizeValue> sizeLimitSetting;
        private final Setting<String> evictionTypeSetting;

        CacheType(
            String cacheTypeName,
            Setting<Boolean> enabledSetting,
            Setting<ByteSizeValue> sizeLimitSetting,
            Setting<String> evictionTypeSetting
        ) {
            this.cacheTypeName = cacheTypeName;
            this.enabledSetting = enabledSetting;
            this.sizeLimitSetting = sizeLimitSetting;
            this.evictionTypeSetting = evictionTypeSetting;
        }

        public boolean isEnabled(ClusterSettings clusterSettings) {
            return clusterSettings.get(enabledSetting);
        }

        public Setting<Boolean> getEnabledSetting() {
            return enabledSetting;
        }

        public Setting<ByteSizeValue> getSizeLimitSetting() {
            return sizeLimitSetting;
        }

        public Setting<String> getEvictionTypeSetting() {
            return evictionTypeSetting;
        }

        public ByteSizeValue getSizeLimit(ClusterSettings clusterSettings) {
            return clusterSettings.get(sizeLimitSetting);
        }

        public String getEvictionType(ClusterSettings clusterSettings) {
            return clusterSettings.get(evictionTypeSetting);
        }

        public String getCacheTypeName() {
            return cacheTypeName;
        }
    }

    /**
     * Creates and configures a CacheManagerConfig pointer with all enabled caches.
     *
     * @param clusterSettings OpenSearch cluster settings containing cache configuration
     * @return CacheManagerConfig pointer ready for runtime creation
     */
    public static long createCacheConfig(ClusterSettings clusterSettings) {
        logger.info("Initializing cache configuration");

        // Initialize empty cache manager config
        long cacheConfigPtr = initCacheManagerConfig();

        // Configure each enabled cache type
        for (CacheType type : CacheType.values()) {
            if (type.isEnabled(clusterSettings)) {
                logger.info("Configuring {} cache: size={} bytes, eviction={}",
                    type.getCacheTypeName(),
                    type.getSizeLimit(clusterSettings).getBytes(),
                    type.getEvictionType(clusterSettings));

                createCache(cacheConfigPtr,type.cacheTypeName, type.getSizeLimit(clusterSettings).getBytes(), type.getEvictionType(clusterSettings));
                clusterSettings.addSettingsUpdateConsumer(type.sizeLimitSetting,(v) ->cacheManagerUpdateSizeLimitForCacheType(type.cacheTypeName,v.getBytes()));
            } else {
                logger.debug("Cache type {} is disabled", type.getCacheTypeName());
            }
        }

        logger.info("Cache configuration complete");
        return cacheConfigPtr;
    }
}
