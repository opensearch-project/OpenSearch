/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.cache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.opensearch.be.datafusion.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.be.datafusion.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.be.datafusion.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;
import static org.opensearch.be.datafusion.cache.CacheSettings.STATISTICS_CACHE_ENABLED;
import static org.opensearch.be.datafusion.cache.CacheSettings.STATISTICS_CACHE_EVICTION_TYPE;
import static org.opensearch.be.datafusion.cache.CacheSettings.STATISTICS_CACHE_SIZE_LIMIT;

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
        METADATA("METADATA", METADATA_CACHE_ENABLED, METADATA_CACHE_SIZE_LIMIT, METADATA_CACHE_EVICTION_TYPE),

        STATISTICS("STATISTICS", STATISTICS_CACHE_ENABLED, STATISTICS_CACHE_SIZE_LIMIT, STATISTICS_CACHE_EVICTION_TYPE);

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
     */
    public static NativeCacheManagerHandle createCacheConfig(ClusterSettings clusterSettings) {
        logger.info("Initializing cache configuration");

        long cacheManagerPtr = NativeBridge.createCustomCacheManager();
        NativeCacheManagerHandle handle = new NativeCacheManagerHandle(cacheManagerPtr);

        // All three caches share the unified METADATA_INDEX_CACHE_TOTAL_SIZE budget.
        // Compute absolute sizes from the percent model BEFORE creating the caches so
        // the footer metadata cache (created in the loop below) gets the correct limit
        // rather than the old standalone METADATA_CACHE_SIZE_LIMIT (hardcoded 250 MB).
        long total = clusterSettings.get(CacheSettings.METADATA_INDEX_CACHE_TOTAL_SIZE);
        int metaPct = clusterSettings.get(CacheSettings.FOOTER_METADATA_CACHE_PERCENT);
        int oiPct = clusterSettings.get(CacheSettings.OFFSET_INDEX_CACHE_PERCENT);
        int ciPct = clusterSettings.get(CacheSettings.COLUMN_INDEX_CACHE_PERCENT);
        int statsPct = clusterSettings.get(CacheSettings.STATISTICS_CACHE_PERCENT);
        long metaLimit = total * metaPct / 100;
        long oiLimit = total * oiPct / 100;
        long ciLimit = total * ciPct / 100;
        long statsLimit = total * statsPct / 100;
        logger.info(
            "Configuring metadata caches: total={} bytes "
                + "(footer={}% → {} bytes, offset_index={}% → {} bytes, column_index={}% → {} bytes, statistics={}% → {} bytes)",
            total,
            metaPct,
            metaLimit,
            oiPct,
            oiLimit,
            ciPct,
            ciLimit,
            statsPct,
            statsLimit
        );

        // Configure each enabled cache type using the percent-derived limit.
        for (CacheType type : CacheType.values()) {
            if (type.isEnabled(clusterSettings)) {
                // Both METADATA and STATISTICS now use the unified budget split.
                long sizeLimit = (type == CacheType.METADATA) ? metaLimit
                    : (type == CacheType.STATISTICS) ? statsLimit
                    : type.getSizeLimit(clusterSettings).getBytes();
                logger.info(
                    "Configuring {} cache: size={} bytes, eviction={}",
                    type.getCacheTypeName(),
                    sizeLimit,
                    type.getEvictionType(clusterSettings)
                );
                NativeBridge.createCache(handle.getPointer(), type.cacheTypeName, sizeLimit, type.getEvictionType(clusterSettings));
            } else {
                logger.debug("Cache type {} is disabled", type.getCacheTypeName());
            }
        }

        NativeBridge.setColumnIndexCacheLimit(ciLimit);
        NativeBridge.setOffsetIndexCacheLimit(oiLimit);
        logger.info("Cache configuration completed");
        return handle;
    }
}
