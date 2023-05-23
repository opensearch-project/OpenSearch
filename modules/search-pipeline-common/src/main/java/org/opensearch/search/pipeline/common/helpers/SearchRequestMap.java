/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common.helpers;

import org.opensearch.action.search.SearchRequest;
import org.opensearch.search.builder.SearchSourceBuilder;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A custom implementation of {@link Map} that provides access to the properties of a {@link SearchRequest}'s
 * {@link SearchSourceBuilder}. The class allows retrieving and modifying specific properties of the search request.
 */
public class SearchRequestMap implements Map<String, Object> {
    private static final String UNSUPPORTED_OP_ERR = " Method not supported in Search pipeline script";

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
     * Retrieves the number of properties in the SearchSourceBuilder.
     *
     * @return The number of properties in the SearchSourceBuilder.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public int size() {
        throw new UnsupportedOperationException("size" + UNSUPPORTED_OP_ERR);
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
     * Checks if the SearchSourceBuilder contains the specified property.
     *
     * @param key The property to check for.
     * @return {@code true} if the SearchSourceBuilder contains the specified property, {@code false} otherwise.
     */
    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    /**
     * Checks if the SearchSourceBuilder contains the specified value.
     *
     * @param value The value to check for.
     * @return {@code true} if the SearchSourceBuilder contains the specified value, {@code false} otherwise.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("containsValue" + UNSUPPORTED_OP_ERR);
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

    /**
     * Removes the specified property from the SearchSourceBuilder.
     *
     * @param key The name of the property that will be removed.
     * @return The value associated with the property before it was removed, or null if the property was not found.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Object remove(Object key) {
        throw new UnsupportedOperationException("remove" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Sets all the properties from the specified map to the SearchSourceBuilder.
     *
     * @param m The map containing the properties to be set.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public void putAll(Map<? extends String, ?> m) {
        throw new UnsupportedOperationException("putAll" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Removes all properties from the SearchSourceBuilder.
     *
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException("clear" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Returns a set view of the property names in the SearchSourceBuilder.
     *
     * @return A set view of the property names in the SearchSourceBuilder.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Set<String> keySet() {
        throw new UnsupportedOperationException("keySet" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Returns a collection view of the property values in the SearchSourceBuilder.
     *
     * @return A collection view of the property values in the SearchSourceBuilder.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Collection<Object> values() {
        throw new UnsupportedOperationException("values" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Returns a set view of the properties in the SearchSourceBuilder.
     *
     * @return A set view of the properties in the SearchSourceBuilder.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Set<Entry<String, Object>> entrySet() {
        throw new UnsupportedOperationException("entrySet" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Returns the value to which the specified property has, or the defaultValue if the property is not present in the
     * SearchSourceBuilder.
     *
     * @param key The property whose associated value is to be returned.
     * @param defaultValue The default value to be returned if the property is not present.
     * @return The value to which the specified property has, or the defaultValue if the property is not present.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        throw new UnsupportedOperationException("getOrDefault" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Performs the given action for each property in the SearchSourceBuilder until all properties have been processed or the
     * action throws an exception
     *
     * @param action The action to be performed for each property.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public void forEach(BiConsumer<? super String, ? super Object> action) {
        throw new UnsupportedOperationException("forEach" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Replaces each property's value with the result of invoking the given function on that property until all properties have
     * been processed or the function throws an exception.
     *
     * @param function The function to apply to each property.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public void replaceAll(BiFunction<? super String, ? super Object, ?> function) {
        throw new UnsupportedOperationException("replaceAll" + UNSUPPORTED_OP_ERR);
    }

    /**
     * If the specified property is not already associated with a value, associates it with the given value and returns null,
     * else returns the current value.
     *
     * @param key The property whose value is to be set if absent.
     * @param value The value to be associated with the specified property.
     * @return The current value associated with the property, or null if the property is not present.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Object putIfAbsent(String key, Object value) {
        throw new UnsupportedOperationException("putIfAbsent" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Removes the property only if it has the given value.
     *
     * @param key The property to be removed.
     * @param value The value expected to be associated with the property.
     * @return {@code true} if the entry was removed, {@code false} otherwise.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException("remove" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Replaces the specified property only if it has the given value.
     *
     * @param key The property to be replaced.
     * @param oldValue The value expected to be associated with the property.
     * @param newValue The value to be associated with the property.
     * @return {@code true} if the property was replaced, {@code false} otherwise.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public boolean replace(String key, Object oldValue, Object newValue) {
        throw new UnsupportedOperationException("replace" + UNSUPPORTED_OP_ERR);
    }

    /**
     * Replaces the specified property only if it has the given value.
     *
     * @param key The property to be replaced.
     * @param value The value to be associated with the property.
     * @return The previous value associated with the property, or null if the property was not found.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Object replace(String key, Object value) {
        throw new UnsupportedOperationException("replace" + UNSUPPORTED_OP_ERR);
    }

    /**
     * The computed value associated with the property, or null if the property is not present.
     *
     * @param key The property whose value is to be computed if absent.
     * @param mappingFunction The function to compute a value based on the property.
     * @return The computed value associated with the property, or null if the property is not present.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Object computeIfAbsent(String key, Function<? super String, ?> mappingFunction) {
        throw new UnsupportedOperationException("computeIfAbsent" + UNSUPPORTED_OP_ERR);
    }

    /**
     * If the value for the specified property is present, attempts to compute a new mapping given the property and its current
     * mapped value.
     *
     * @param key The property for which the mapping is to be computed.
     * @param remappingFunction The function to compute a new mapping.
     * @return The new value associated with the property, or null if the property is not present.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Object computeIfPresent(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException("computeIfPresent" + UNSUPPORTED_OP_ERR);
    }

    /**
     * If the value for the specified property is present, attempts to compute a new mapping given the property and its current
     * mapped value, or removes the property if the computed value is null.
     *
     * @param key The property for which the mapping is to be computed.
     * @param remappingFunction The function to compute a new mapping.
     * @return The new value associated with the property, or null if the property is not present.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Object compute(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException("compute" + UNSUPPORTED_OP_ERR);
    }

    /**
     * If the specified property is not already associated with a value or is associated with null, associates it with the
     * given non-null value. Otherwise, replaces the associated value with the results of applying the given
     * remapping function to the current and new values.
     *
     * @param key The property for which the mapping is to be merged.
     * @param value The non-null value to be merged with the existing value.
     * @param remappingFunction The function to merge the existing and new values.
     * @return The new value associated with the property, or null if the property is not present.
     * @throws UnsupportedOperationException always, as the method is not supported.
     */
    @Override
    public Object merge(String key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException("merge" + UNSUPPORTED_OP_ERR);
    }
}
