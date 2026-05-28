/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Registry that maps field-domain metadata type names to parsers.
 */
public final class FieldDomainParserRegistry {
    private static final FieldDomainParserRegistry DEFAULT = new FieldDomainParserRegistry(List.of(new DateRangeFieldDomainParser()));

    private final Map<String, FieldDomainParser> parsers;

    /**
     * Creates a registry from the supplied parsers.
     *
     * @param parsers parsers keyed by their {@link FieldDomainParser#type()}
     */
    public FieldDomainParserRegistry(List<FieldDomainParser> parsers) {
        Objects.requireNonNull(parsers, "parsers must not be null");

        Map<String, FieldDomainParser> byType = new LinkedHashMap<>();
        for (FieldDomainParser parser : parsers) {
            Objects.requireNonNull(parser, "parser must not be null");
            FieldDomainParser previous = byType.put(parser.type(), parser);
            if (previous != null) {
                throw new IllegalArgumentException("field domain parser [" + parser.type() + "] is already registered");
            }
        }
        this.parsers = Map.copyOf(byType);
    }

    /**
     * Returns the built-in parser registry.
     */
    public static FieldDomainParserRegistry defaultRegistry() {
        return DEFAULT;
    }

    /**
     * Returns the parser registered for a metadata type.
     */
    public Optional<FieldDomainParser> get(String type) {
        return Optional.ofNullable(parsers.get(type));
    }

    /**
     * Removes all known parser-owned keys for a field prefix.
     */
    public void removeFieldKeys(Map<String, String> target, String prefix) {
        for (FieldDomainParser parser : parsers.values()) {
            parser.removeFieldKeys(target, prefix);
        }
    }
}
