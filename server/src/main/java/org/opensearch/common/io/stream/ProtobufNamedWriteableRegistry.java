/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.common.io.stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A registry for {@link org.opensearch.common.io.stream.ProtobufWriteable.Reader} readers of {@link ProtobufNamedWriteable}.
*
* The registration is keyed by the combination of the category class of {@link ProtobufNamedWriteable}, and a name unique
* to that category.
*
* @opensearch.internal
*/
public class ProtobufNamedWriteableRegistry {

    /**
     * An entry in the registry, made up of a category class and name, and a reader for that category class.
    *
    * @opensearch.internal
    */
    public static class Entry {

        /** The superclass of a {@link ProtobufNamedWriteable} which will be read by {@link #reader}. */
        public final Class<?> categoryClass;

        /** A name for the writeable which is unique to the {@link #categoryClass}. */
        public final String name;

        /** A reader capability of reading*/
        public final ProtobufWriteable.Reader<?> reader;

        /** Creates a new entry which can be stored by the registry. */
        public <T extends ProtobufNamedWriteable> Entry(Class<T> categoryClass, String name, ProtobufWriteable.Reader<? extends T> reader) {
            this.categoryClass = Objects.requireNonNull(categoryClass);
            this.name = Objects.requireNonNull(name);
            this.reader = Objects.requireNonNull(reader);
        }
    }

    /**
     * The underlying data of the registry maps from the category to an inner
    * map of name unique to that category, to the actual reader.
    */
    private final Map<Class<?>, Map<String, ProtobufWriteable.Reader<?>>> registry;

    /**
     * Constructs a new registry from the given entries.
    */
    public ProtobufNamedWriteableRegistry(List<Entry> entries) {
        if (entries.isEmpty()) {
            registry = Collections.emptyMap();
            return;
        }
        entries = new ArrayList<>(entries);
        entries.sort((e1, e2) -> e1.categoryClass.getName().compareTo(e2.categoryClass.getName()));

        Map<Class<?>, Map<String, ProtobufWriteable.Reader<?>>> registry = new HashMap<>();
        Map<String, ProtobufWriteable.Reader<?>> readers = null;
        Class currentCategory = null;
        for (Entry entry : entries) {
            if (currentCategory != entry.categoryClass) {
                if (currentCategory != null) {
                    // we've seen the last of this category, put it into the big map
                    registry.put(currentCategory, Collections.unmodifiableMap(readers));
                }
                readers = new HashMap<>();
                currentCategory = entry.categoryClass;
            }

            ProtobufWriteable.Reader<?> oldReader = readers.put(entry.name, entry.reader);
            if (oldReader != null) {
                throw new IllegalArgumentException(
                    "ProtobufNamedWriteable ["
                        + currentCategory.getName()
                        + "]["
                        + entry.name
                        + "]"
                        + " is already registered for ["
                        + oldReader.getClass().getName()
                        + "],"
                        + " cannot register ["
                        + entry.reader.getClass().getName()
                        + "]"
                );
            }
        }
        // handle the last category
        registry.put(currentCategory, Collections.unmodifiableMap(readers));

        this.registry = Collections.unmodifiableMap(registry);
    }

    /**
     * Returns a reader for a {@link ProtobufNamedWriteable} object identified by the
    * name provided as argument and its category.
    */
    public <T> ProtobufWriteable.Reader<? extends T> getReader(Class<T> categoryClass, String name) {
        Map<String, ProtobufWriteable.Reader<?>> readers = registry.get(categoryClass);
        if (readers == null) {
            throw new IllegalArgumentException("Unknown ProtobufNamedWriteable category [" + categoryClass.getName() + "]");
        }
        @SuppressWarnings("unchecked")
        ProtobufWriteable.Reader<? extends T> reader = (ProtobufWriteable.Reader<? extends T>) readers.get(name);
        if (reader == null) {
            throw new IllegalArgumentException("Unknown ProtobufNamedWriteable [" + categoryClass.getName() + "][" + name + "]");
        }
        return reader;
    }
}
