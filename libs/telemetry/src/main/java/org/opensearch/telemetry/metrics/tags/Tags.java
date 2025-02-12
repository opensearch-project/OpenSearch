/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.telemetry.metrics.tags;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Class to create tags for a meter.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class Tags {
    private final Map<String, Object> tagsMap;
    /**
     * Empty value.
     */
    public final static Tags EMPTY = new Tags(Collections.emptyMap());

    /**
     * Factory method.
     * @return tags.
     */
    public static Tags create() {
        return new Tags(new HashMap<>());
    }

    /**
     * Constructor.
     */
    private Tags(Map<String, Object> tagsMap) {
        this.tagsMap = tagsMap;
    }

    /**
     * Add String attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Tags addTag(String key, String value) {
        Objects.requireNonNull(value, "value cannot be null");
        tagsMap.put(key, value);
        return this;
    }

    /**
     * Add long attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Tags addTag(String key, long value) {
        tagsMap.put(key, value);
        return this;
    };

    /**
     * Add double attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Tags addTag(String key, double value) {
        tagsMap.put(key, value);
        return this;
    };

    /**
     * Add boolean attribute.
     * @param key key
     * @param value value
     * @return Same instance.
     */
    public Tags addTag(String key, boolean value) {
        tagsMap.put(key, value);
        return this;
    };

    /**
     * Returns the attribute map.
     * @return tags map
     */
    public Map<String, ?> getTagsMap() {
        return Collections.unmodifiableMap(tagsMap);
    }

}
