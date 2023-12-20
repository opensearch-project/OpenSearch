/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.model;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

/**
 * Simple abstract class that represent record stored in the Query Insight Framework
 *
 * @param <T> The value type associated with the record
 * @opensearch.internal
 */
public abstract class SearchQueryRecord<T extends Number & Comparable<T>>
    implements
        Comparable<SearchQueryRecord<T>>,
        Writeable,
        ToXContentObject {

    private static final Logger log = LogManager.getLogger(SearchQueryRecord.class);
    protected static final String TIMESTAMP = "timestamp";
    protected static final String SEARCH_TYPE = "searchType";
    protected static final String SOURCE = "source";
    protected static final String TOTAL_SHARDS = "totalShards";
    protected static final String INDICES = "indices";
    protected static final String PROPERTY_MAP = "propertyMap";
    protected static final String VALUE = "value";

    protected final Long timestamp;

    private final SearchType searchType;

    private final String source;

    private final int totalShards;

    private final String[] indices;

    // TODO: add user-account which initialized the request in the future
    private final Map<String, Object> propertyMap;

    private T value;

    public SearchQueryRecord(final StreamInput in) throws IOException, ClassCastException {
        this.timestamp = in.readLong();
        this.searchType = SearchType.fromString(in.readString().toLowerCase(Locale.ROOT));
        this.source = in.readString();
        this.totalShards = in.readInt();
        this.indices = in.readStringArray();
        this.propertyMap = in.readMap();
        this.value = castToValue(in.readGenericValue());
    }

    public SearchQueryRecord(
        final Long timestamp,
        final SearchType searchType,
        final String source,
        final int totalShards,
        final String[] indices,
        final Map<String, Object> propertyMap,
        final T value
    ) {
        this(timestamp, searchType, source, totalShards, indices, propertyMap);
        this.value = value;
    }

    public SearchQueryRecord(
        final Long timestamp,
        final SearchType searchType,
        final String source,
        final int totalShards,
        final String[] indices,
        final Map<String, Object> propertyMap
    ) {
        this.timestamp = timestamp;
        this.searchType = searchType;
        this.source = source;
        this.totalShards = totalShards;
        this.indices = indices;
        this.propertyMap = propertyMap;
    }

    /**
     * The timestamp of the top query.
     */
    public Long getTimestamp() {
        return timestamp;
    }

    /**
     * The manner at which the search operation is executed.
     */
    public SearchType getSearchType() {
        return searchType;
    }

    /**
     * The search source that was executed by the query.
     */
    public String getSource() {
        return source;
    }

    /**
     * Total number of shards as part of the search query across all indices
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The indices involved in the search query
     */
    public String[] getIndices() {
        return indices;
    }

    /**
     * Get the value of the query metric record
     */
    public T getValue() {
        return value;
    }

    /**
     * Set the value of the query metric record
     */
    public void setValue(T value) {
        this.value = value;
    }

    /**
     * Extra attributes and information about a search query
     */
    public Map<String, Object> getPropertyMap() {
        return propertyMap;
    }

    @Override
    public int compareTo(SearchQueryRecord<T> otherRecord) {
        return value.compareTo(otherRecord.getValue());
    }

    public boolean equals(SearchQueryRecord<T> other) {
        if (false == this.timestamp.equals(other.getTimestamp())
            && this.searchType.equals(other.getSearchType())
            && this.source.equals(other.getSource())
            && this.totalShards == other.getTotalShards()
            && this.indices.length == other.getIndices().length
            && this.propertyMap.size() == other.getPropertyMap().size()
            && this.value.equals(other.getValue())) {
            return false;
        }
        for (int i = 0; i < indices.length; i++) {
            if (!indices[i].equals(other.getIndices()[i])) {
                return false;
            }
        }
        for (String key : propertyMap.keySet()) {
            if (!other.getPropertyMap().containsKey(key)) {
                return false;
            }
            if (!propertyMap.get(key).equals(other.getPropertyMap().get(key))) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private T castToValue(Object obj) throws ClassCastException {
        try {
            return (T) obj;
        } catch (Exception e) {
            log.error(String.format(Locale.ROOT, "error casting query insight record value, error: %s", e));
            throw e;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TIMESTAMP, timestamp);
        builder.field(SEARCH_TYPE, searchType);
        builder.field(SOURCE, source);
        builder.field(TOTAL_SHARDS, totalShards);
        builder.field(INDICES, indices);
        builder.field(PROPERTY_MAP, propertyMap);
        builder.field(VALUE, value);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeString(searchType.toString());
        out.writeString(source);
        out.writeInt(totalShards);
        out.writeStringArray(indices);
        out.writeMap(propertyMap);
        out.writeGenericValue(value);
    }
}
