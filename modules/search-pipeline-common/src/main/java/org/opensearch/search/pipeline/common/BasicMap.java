/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Helper for map abstractions passed to scripting processors. Throws {@link UnsupportedOperationException} for almost
 * all methods. Subclasses just need to implement get and put.
 */
abstract class BasicMap implements Map<String, Object> {

    /**
     * No-args constructor.
     */
    protected BasicMap() {}

    private static final String UNSUPPORTED_OP_ERR = " Method not supported in Search pipeline script";

    @Override
    public boolean isEmpty() {
        throw new UnsupportedOperationException("isEmpty" + UNSUPPORTED_OP_ERR);
    }

    public int size() {
        throw new UnsupportedOperationException("size" + UNSUPPORTED_OP_ERR);
    }

    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    public boolean containsValue(Object value) {
        throw new UnsupportedOperationException("containsValue" + UNSUPPORTED_OP_ERR);
    }

    public Object remove(Object key) {
        throw new UnsupportedOperationException("remove" + UNSUPPORTED_OP_ERR);
    }

    public void putAll(Map<? extends String, ?> m) {
        throw new UnsupportedOperationException("putAll" + UNSUPPORTED_OP_ERR);
    }

    public void clear() {
        throw new UnsupportedOperationException("clear" + UNSUPPORTED_OP_ERR);
    }

    public Set<String> keySet() {
        throw new UnsupportedOperationException("keySet" + UNSUPPORTED_OP_ERR);
    }

    public Collection<Object> values() {
        throw new UnsupportedOperationException("values" + UNSUPPORTED_OP_ERR);
    }

    public Set<Map.Entry<String, Object>> entrySet() {
        throw new UnsupportedOperationException("entrySet" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public Object getOrDefault(Object key, Object defaultValue) {
        throw new UnsupportedOperationException("getOrDefault" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public void forEach(BiConsumer<? super String, ? super Object> action) {
        throw new UnsupportedOperationException("forEach" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public void replaceAll(BiFunction<? super String, ? super Object, ?> function) {
        throw new UnsupportedOperationException("replaceAll" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public Object putIfAbsent(String key, Object value) {
        throw new UnsupportedOperationException("putIfAbsent" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public boolean remove(Object key, Object value) {
        throw new UnsupportedOperationException("remove" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public boolean replace(String key, Object oldValue, Object newValue) {
        throw new UnsupportedOperationException("replace" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public Object replace(String key, Object value) {
        throw new UnsupportedOperationException("replace" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public Object computeIfAbsent(String key, Function<? super String, ?> mappingFunction) {
        throw new UnsupportedOperationException("computeIfAbsent" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public Object computeIfPresent(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException("computeIfPresent" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public Object compute(String key, BiFunction<? super String, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException("compute" + UNSUPPORTED_OP_ERR);
    }

    @Override
    public Object merge(String key, Object value, BiFunction<? super Object, ? super Object, ?> remappingFunction) {
        throw new UnsupportedOperationException("merge" + UNSUPPORTED_OP_ERR);
    }
}
