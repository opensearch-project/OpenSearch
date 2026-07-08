/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Registry that maps field-domain metadata type names to parsers.
 *
 * The default registry currently contains only built-in parsers. Plugin registration for additional parser and evaluator
 * implementations is not wired yet.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class FieldDomainParserRegistry {

    private static final FieldDomainParserRegistry DEFAULT = new FieldDomainParserRegistry(
        List.of(entry(DateRangeFieldDomain.class, new DateRangeFieldDomainParser()))
    );

    private final Map<String, Entry<? extends FieldDomain>> entries;

    /**
     * Creates a registry from the supplied parser entries.
     *
     * @param entries parser entries keyed by their {@link FieldDomainParser#type()}
     */
    public FieldDomainParserRegistry(List<Entry<? extends FieldDomain>> entries) {
        Objects.requireNonNull(entries, "entries must not be null");

        Map<String, Entry<? extends FieldDomain>> typeToParser = new LinkedHashMap<>();
        for (Entry<? extends FieldDomain> entry : entries) {
            Objects.requireNonNull(entry, "entry must not be null");
            Entry<? extends FieldDomain> previous = typeToParser.put(entry.type(), entry);
            if (previous != null) {
                throw new IllegalArgumentException("field domain parser [" + entry.type() + "] is already registered");
            }
        }
        this.entries = Map.copyOf(typeToParser);
    }

    /**
     * Creates a typed parser entry for registration.
     */
    public static <T extends FieldDomain> Entry<T> entry(Class<T> domainClass, FieldDomainParser<T> parser) {
        return new Entry<>(domainClass, parser);
    }

    /**
     * Returns the built-in parser registry.
     */
    public static FieldDomainParserRegistry defaultRegistry() {
        return DEFAULT;
    }

    /**
     * Returns whether a parser is registered for a metadata type.
     */
    public boolean contains(String type) {
        return entries.containsKey(type);
    }

    /**
     * Reads field-domain metadata with the parser registered for the supplied type.
     */
    public Optional<FieldDomain> fromCustomData(String type, String field, Map<String, String> customData, String prefix) {
        Entry<? extends FieldDomain> entry = entries.get(type);
        if (entry == null) {
            return Optional.empty();
        }
        return entry.fromCustomData(field, customData, prefix);
    }

    /**
     * Writes a field domain with the parser registered for its metadata type.
     */
    public void writeToCustomData(FieldDomain domain, Map<String, String> targetCustomData, String prefix) {
        Objects.requireNonNull(domain, "domain must not be null");

        Entry<? extends FieldDomain> entry = entries.get(domain.type());
        if (entry == null) {
            throw new IllegalArgumentException("unsupported field domain type [" + domain.type() + "]");
        }
        entry.writeToCustomData(domain, targetCustomData, prefix);
    }

    /**
     * Removes all known parser-owned keys for a field prefix.
     */
    public void removeFieldKeys(Map<String, String> target, String prefix) {
        for (Entry<? extends FieldDomain> entry : entries.values()) {
            entry.removeFieldKeys(target, prefix);
        }
    }

    /**
     * Typed parser registration entry.
     *
     * @opensearch.experimental
     */
    @ExperimentalApi
    public static final class Entry<T extends FieldDomain> {
        private final Class<T> domainClass;
        private final FieldDomainParser<T> parser;

        private Entry(Class<T> domainClass, FieldDomainParser<T> parser) {
            this.domainClass = Objects.requireNonNull(domainClass, "domainClass must not be null");
            this.parser = Objects.requireNonNull(parser, "parser must not be null");
        }

        private String type() {
            return parser.type();
        }

        private Optional<FieldDomain> fromCustomData(String field, Map<String, String> customData, String prefix) {
            return parser.fromCustomData(field, customData, prefix).map(FieldDomain.class::cast);
        }

        private void writeToCustomData(FieldDomain domain, Map<String, String> targetCustomData, String prefix) {
            if (domainClass.isInstance(domain) == false) {
                throw new IllegalArgumentException(
                    "field domain class [" + domain.getClass().getName() + "] is not supported by parser [" + type() + "]"
                );
            }
            parser.writeToCustomData(domainClass.cast(domain), targetCustomData, prefix);
        }

        private void removeFieldKeys(Map<String, String> target, String prefix) {
            parser.removeFieldKeys(target, prefix);
        }
    }
}
