package org.opensearch.datafusion.search.cache;

import java.util.Map;
import java.util.List;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.core.common.unit.ByteSizeValue;

public abstract class CacheAccessor {

    protected CacheType name;
    protected Map<String, Object> properties;
    protected long pointer;
    protected long sizeLimit;

    public void setSizeLimit(ByteSizeValue sizeLimit) {
        this.sizeLimit = sizeLimit.getBytes();
    }

    public long getPointer() {
        return pointer;
    }

    public long getConfiguredSizeLimit() {
        return this.sizeLimit;
    }

    public String getName() {
        return name.toString();
    }

    public CacheAccessor(long cacheManagerPointer, ClusterSettings cacheSettings, CacheType name) {
        this.properties = extractSettings(cacheSettings);
        this.pointer = createCache(cacheManagerPointer, properties);
        this.name = name;
    }

    // Abstract method - subclasses define what settings to extract
    protected abstract Map<String, Object> extractSettings(ClusterSettings clusterSettings);
    public abstract long createCache(long cacheManagerPointer, Map<String, Object> properties);

    // Instance methods matching native signatures
    public abstract boolean put(String filePath);
    public abstract Object get(String filePath);
    public abstract boolean remove(String filePath);
    public abstract void evict();
    public abstract void clear();
    public abstract long getMemoryConsumed();
    public abstract boolean containsFile(String filePath);
    public abstract List<String> getEntries();
}
