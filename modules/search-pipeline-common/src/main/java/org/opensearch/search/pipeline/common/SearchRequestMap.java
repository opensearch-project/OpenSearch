/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Map;

/**
 * A custom implementation of {@link Map} that provides access to the properties of a {@link SearchRequest}'s
 * {@link SearchSourceBuilder}. The class allows retrieving and modifying specific properties of the search request.
 */
class SearchRequestMap extends BasicMap implements Map<String, Object> {

    private final SearchSourceBuilder source;

    /**
     * Constructs a new instance of the {@link SearchRequestMap} with the provided {@link SearchRequest}.
     *
     * @param searchRequest The SearchRequest containing the SearchSourceBuilder to be accessed.
     */
    public SearchRequestMap(SearchRequest searchRequest) {
        source = searchRequest.source();
    }

    /**
     * Checks if the SearchSourceBuilder is empty.
     *
     * @return {@code true} if the SearchSourceBuilder is empty, {@code false} otherwise.
     */
    @Override
    public boolean isEmpty() {
        return source == null;
    }

    /**
     * Retrieves the value associated with the specified property from the SearchSourceBuilder.
     *
     * @param key The SearchSourceBuilder property whose value is to be retrieved.
     * @return The value associated with the specified property or null if the property has not been initialized.
     * @throws IllegalArgumentException if the property name is not a String.
     * @throws SearchRequestMapProcessingException if the property is not supported.
     */
    @Override
    public Object get(Object key) {
        if (!(key instanceof String)) {
            throw new IllegalArgumentException("key must be a String");
        }
        // This is the explicit implementation of fetch value from source
        switch ((String) key) {
            case "from":
                return source.from();
            case "size":
                return source.size();
            case "explain":
                return source.explain();
            case "version":
                return source.version();
            case "seq_no_primary_term":
                return source.seqNoAndPrimaryTerm();
            case "track_scores":
                return source.trackScores();
            case "track_total_hits":
                return source.trackTotalHitsUpTo();
            case "min_score":
                return source.minScore();
            case "terminate_after":
                return source.terminateAfter();
            case "profile":
                return source.profile();
            default:
                throw new SearchRequestMapProcessingException("Unsupported key: " + key);
        }
    }

    /**
     * Sets the value for the specified property in the SearchSourceBuilder.
     *
     * @param key The property whose value is to be set.
     * @param value The value to be set for the specified property.
     * @return The original value associated with the property, or null if none existed.
     * @throws IllegalArgumentException if the property is not a String.
     * @throws SearchRequestMapProcessingException if the property is not supported or an error occurs during the setting.
     */
    @Override
    public Object put(String key, Object value) {
        Object originalValue = get(key);
        try {
            switch (key) {
                case "from":
                    source.from((Integer) value);
                    break;
                case "size":
                    source.size((Integer) value);
                    break;
                case "explain":
                    source.explain((Boolean) value);
                    break;
                case "version":
                    source.version((Boolean) value);
                    break;
                case "seq_no_primary_term":
                    source.seqNoAndPrimaryTerm((Boolean) value);
                    break;
                case "track_scores":
                    source.trackScores((Boolean) value);
                    break;
                case "track_total_hits":
                    source.trackTotalHitsUpTo((Integer) value);
                    break;
                case "min_score":
                    source.minScore((Float) value);
                    break;
                case "terminate_after":
                    source.terminateAfter((Integer) value);
                    break;
                case "profile":
                    source.profile((Boolean) value);
                    break;
                case "stats": // Not modifying stats, sorts, docvalue_fields, etc. as they require more complex handling
                case "sort":
                case "timeout":
                case "docvalue_fields":
                case "indices_boost":
                default:
                    throw new SearchRequestMapProcessingException("Unsupported SearchRequest source property: " + key);
            }
        } catch (Exception e) {
            throw new SearchRequestMapProcessingException("Error while setting value for SearchRequest source property: " + key, e);
        }
        return originalValue;
    }
}
