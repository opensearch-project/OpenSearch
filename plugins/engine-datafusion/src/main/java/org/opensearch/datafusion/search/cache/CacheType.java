package org.opensearch.datafusion.search.cache;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;

import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_ENABLED;

public enum CacheType {
    METADATA(MetadataCacheAccessor::new, METADATA_CACHE_ENABLED);
   // STATS(StatsCacheAccessor::new);

    private final CacheFactory factory;
    private final Setting<Boolean> enabledSetting;

    CacheType(CacheFactory factory, Setting<Boolean> enabledSetting) {
        this.factory = factory;
        this.enabledSetting = enabledSetting;
    }

    public CacheAccessor createCache(long cacheManagerPointer, ClusterSettings clusterSettings) {
        return factory.create(cacheManagerPointer, clusterSettings, this);
    }

    public boolean isEnabled(ClusterSettings clusterSettings) {
        return clusterSettings.get(enabledSetting);
    }

    @FunctionalInterface
    private interface CacheFactory {
        CacheAccessor create(long cacheManagerPointer, ClusterSettings clusterSettings, CacheType type);
    }
}
