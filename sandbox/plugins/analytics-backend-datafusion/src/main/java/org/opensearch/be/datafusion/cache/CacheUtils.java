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
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;

import static org.opensearch.be.datafusion.cache.CacheSettings.METADATA_CACHE_ENABLED;
import static org.opensearch.be.datafusion.cache.CacheSettings.STATISTICS_CACHE_ENABLED;

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
        METADATA("METADATA", METADATA_CACHE_ENABLED) {
            @Override
            public long sizeBytes(long metaLimit, long oiLimit, long ciLimit, long statsLimit) {
                return metaLimit;
            }
        },
        STATISTICS("STATISTICS", STATISTICS_CACHE_ENABLED) {
            @Override
            public long sizeBytes(long metaLimit, long oiLimit, long ciLimit, long statsLimit) {
                return statsLimit;
            }
        },
        COLUMN_INDEX("COLUMN_INDEX", null) {
            @Override
            public long sizeBytes(long metaLimit, long oiLimit, long ciLimit, long statsLimit) {
                return ciLimit;
            }

            @Override
            public boolean isEnabled(ClusterSettings clusterSettings) {
                return clusterSettings.get(org.opensearch.be.datafusion.DataFusionPlugin.SCOPED_PAGE_INDEX_ENABLED);
            }
        },
        OFFSET_INDEX("OFFSET_INDEX", null) {
            @Override
            public long sizeBytes(long metaLimit, long oiLimit, long ciLimit, long statsLimit) {
                return oiLimit;
            }

            @Override
            public boolean isEnabled(ClusterSettings clusterSettings) {
                return clusterSettings.get(org.opensearch.be.datafusion.DataFusionPlugin.SCOPED_PAGE_INDEX_ENABLED);
            }
        };

        private final String cacheTypeName;
        @Nullable
        private final Setting<Boolean> enabledSetting;

        CacheType(String cacheTypeName, @Nullable Setting<Boolean> enabledSetting) {
            this.cacheTypeName = cacheTypeName;
            this.enabledSetting = enabledSetting;
        }

        public abstract long sizeBytes(long metaLimit, long oiLimit, long ciLimit, long statsLimit);

        public boolean isEnabled(ClusterSettings clusterSettings) {
            return enabledSetting != null && clusterSettings.get(enabledSetting);
        }

        @Nullable
        public Setting<Boolean> getEnabledSetting() {
            return enabledSetting;
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
        long total = clusterSettings.get(CacheSettings.METADATA_INDEX_CACHE_TOTAL_SIZE).getBytes();
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

        // Configure each enabled cache type using the percent-derived limit. Eviction policy is
        // fixed at S3-FIFO (scan-resistant) in native code — no longer configurable per cache.
        for (CacheType type : CacheType.values()) {
            if (type.isEnabled(clusterSettings)) {
                long sizeLimit = type.sizeBytes(metaLimit, oiLimit, ciLimit, statsLimit);
                logger.info("Configuring {} cache: size={} bytes, eviction=S3FIFO", type.getCacheTypeName(), sizeLimit);
                NativeBridge.createCache(handle.getPointer(), type.cacheTypeName, sizeLimit);
            } else {
                logger.debug("Cache type {} is disabled", type.getCacheTypeName());
            }
        }

        logger.info("Cache configuration completed");
        return handle;
    }
}
