/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

import java.util.Map;
import java.util.Optional;

/**
 * Parser and encoder for one {@link FieldDomain} metadata type.
 *
 * Implementations convert between typed field-domain objects and the flat {@code Map<String, String>} stored in
 * {@link org.opensearch.cluster.metadata.IndexMetadata} custom data.
 */
public interface FieldDomainParser {
    /**
     * Metadata type handled by this parser.
     */
    String type();

    /**
     * Parses bounds for a field from custom metadata.
     *
     * @param field field name being parsed
     * @param customData full custom metadata map for {@code index_field_domains}
     * @param prefix field-specific metadata prefix
     * @return parsed domain when the metadata is complete and valid; otherwise empty
     */
    Optional<FieldDomain> parse(String field, Map<String, String> customData, String prefix);

    /**
     * Encodes bounds into a target metadata map.
     *
     * @param domain domain to encode
     * @param target target metadata map
     * @param prefix field-specific metadata prefix
     */
    void encode(FieldDomain domain, Map<String, String> target, String prefix);

    /**
     * Removes keys owned by this parser for one field from a metadata map.
     */
    void removeFieldKeys(Map<String, String> target, String prefix);
}
