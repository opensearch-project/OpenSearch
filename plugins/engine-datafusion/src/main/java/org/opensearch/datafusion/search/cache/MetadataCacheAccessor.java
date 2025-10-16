package org.opensearch.datafusion.search.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.common.unit.ByteSizeValue;

import static org.opensearch.datafusion.DataFusionQueryJNI.createMetadataCache;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheClear;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheContainsFile;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheGet;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheGetEntries;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheGetSize;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCachePut;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheRemove;
import static org.opensearch.datafusion.DataFusionQueryJNI.metadataCacheUpdateSizeLimit;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_EVICTION_TYPE;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT;
import static org.opensearch.datafusion.search.cache.CacheSettings.METADATA_CACHE_SIZE_LIMIT_KEY;

public class MetadataCacheAccessor extends CacheAccessor {
    private CachePolicy cachePolicy;

    public MetadataCacheAccessor(long cacheManagerPointer, ClusterSettings settings, CacheType type) {
        super(cacheManagerPointer, settings,type);
    }

    public void setCachePolicy(String cachePolicy) {
        this.cachePolicy = CachePolicy.valueOf(cachePolicy);
    }

    @Override
    protected Map<String, Object> extractSettings(ClusterSettings clusterSettings) {
        Map<String, Object> properties = new HashMap<>();

        clusterSettings.addSettingsUpdateConsumer(METADATA_CACHE_SIZE_LIMIT, this::setSizeLimit);
        setSizeLimit(clusterSettings.get(METADATA_CACHE_SIZE_LIMIT));
        properties.put(METADATA_CACHE_SIZE_LIMIT_KEY,this.sizeLimit);

        clusterSettings.addSettingsUpdateConsumer(METADATA_CACHE_EVICTION_TYPE, this::setCachePolicy);
        setCachePolicy(clusterSettings.get(METADATA_CACHE_EVICTION_TYPE));
        properties.put(METADATA_CACHE_EVICTION_TYPE.getKey(),this.cachePolicy);

        return properties;
    }

    @Override
    public long createCache(long cacheManagerPointer, Map<String, Object> properties) {
        return createMetadataCache(cacheManagerPointer, this.sizeLimit);
    }

    @Override
    public boolean put(String filePath) {
        return metadataCachePut(this.getPointer(), filePath);
    }

    @Override
    public Object get(String filePath) {
        return metadataCacheGet(this.pointer,filePath);
    }

    @Override
    public boolean remove(String filePath) {
        return metadataCacheRemove(this.pointer, filePath);
    }

    @Override
    public void evict() {
        throw new UnsupportedOperationException("Explicit Eviction Not Supported");
    }

    @Override
    public void clear() {
        metadataCacheClear(this.pointer);
    }

    @Override
    public long getMemoryConsumed() {
        return metadataCacheGetSize(this.pointer);
    }

    @Override
    public boolean containsFile(String filePath) {
        return metadataCacheContainsFile(this.pointer, filePath);
    }

    // TODO: Replace the logic with optimized version to check if it is update or set limit call
    @Override
    public void setSizeLimit(ByteSizeValue limit) {
        if(this.sizeLimit == 0){
            this.sizeLimit = limit.getBytes();
        } else{
            metadataCacheUpdateSizeLimit(this.pointer, limit.getBytes());
            this.sizeLimit = limit.getBytes();
        }
    }

    @Override
    public List<String> getEntries() {
       return List.of(metadataCacheGetEntries(this.pointer));
    }

}
