/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddomain;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Map;
import java.util.Optional;

/**
 * Reader and writer for one {@link FieldDomain} metadata type.
 *
 * Implementations convert between typed field-domain objects and the flat {@code Map<String, String>} stored in
 * {@link org.opensearch.cluster.metadata.IndexMetadata} custom data.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface FieldDomainParser<T extends FieldDomain> {
    /**
     * Metadata type handled by this parser.
     */
    String type();

    /**
     * Reads a field domain from custom metadata.
     *
     * @param field field name being parsed
     * @param customData full custom metadata map for {@code index_field_domains}
     * @param prefix field-specific metadata prefix
     * @return field domain when the metadata is complete and valid; otherwise empty
     */
    Optional<T> fromCustomData(String field, Map<String, String> customData, String prefix);

    /**
     * Writes a field domain into a target custom metadata map.
     *
     * @param domain domain to write
     * @param targetCustomData target metadata map
     * @param prefix field-specific metadata prefix
     */
    void writeToCustomData(T domain, Map<String, String> targetCustomData, String prefix);

    /**
     * Removes keys owned by this parser for one field from a metadata map.
     */
    void removeFieldKeys(Map<String, String> target, String prefix);
}
