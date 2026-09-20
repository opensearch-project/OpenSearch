/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.apache.lucene.util.automaton.Operations;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.regex.Regex;

import java.util.Objects;

/**
 * Describes a system index. Provides the information required to create and maintain the system index.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.16.0")
public class SystemIndexDescriptor {
    private final String indexPattern;
    private final String description;
    private final CharacterRunAutomaton indexPatternAutomaton;
    private final String primaryIndex;
    private final String writeAlias;
    private final String mappings;
    private final long mappingVersion;

    /**
     *
     * @param indexPattern The pattern of index names that this descriptor will be used for. Must start with a '.' character.
     * @param description The name of the plugin responsible for this system index.
     */
    public SystemIndexDescriptor(String indexPattern, String description) {
        this(indexPattern, description, null, null, null);
    }

    private SystemIndexDescriptor(
        String indexPattern,
        String description,
        @Nullable String primaryIndex,
        @Nullable String writeAlias,
        @Nullable String mappings
    ) {
        Objects.requireNonNull(indexPattern, "system index pattern must not be null");
        if (indexPattern.length() < 2) {
            throw new IllegalArgumentException(
                "system index pattern provided as [" + indexPattern + "] but must at least 2 characters in length"
            );
        }
        if (indexPattern.charAt(0) != '.') {
            throw new IllegalArgumentException(
                "system index pattern provided as [" + indexPattern + "] but must start with the character [.]"
            );
        }
        if (indexPattern.charAt(1) == '*') {
            throw new IllegalArgumentException(
                "system index pattern provided as ["
                    + indexPattern
                    + "] but must not start with the character sequence [.*] to prevent conflicts"
            );
        }
        this.indexPattern = indexPattern;
        Automaton a = Operations.determinize(Regex.simpleMatchToAutomaton(indexPattern), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
        this.indexPatternAutomaton = new CharacterRunAutomaton(a);
        this.description = description;
        this.primaryIndex = primaryIndex;
        this.writeAlias = writeAlias;
        this.mappings = mappings;
        this.mappingVersion = mappings == null
            ? SystemIndexMappingUpdater.NO_SCHEMA_VERSION
            : SystemIndexMappingUpdater.getMappingVersion(mappings);

        if (primaryIndex != null && matchesIndexPattern(primaryIndex) == false) {
            throw new IllegalArgumentException(
                "primary index [" + primaryIndex + "] must match system index pattern [" + indexPattern + "]"
            );
        }
    }

    /**
     * Creates a builder for a system index descriptor.
     *
     * @param indexPattern The pattern of index names that this descriptor will be used for.
     * @param description A short description of the system index.
     * @return A new descriptor builder.
     */
    public static Builder builder(String indexPattern, String description) {
        return new Builder(indexPattern, description);
    }

    /**
     * @return The pattern of index names that this descriptor will be used for.
     */
    public String getIndexPattern() {
        return indexPattern;
    }

    /**
     * Checks whether an index name matches the system index name pattern for this descriptor.
     * @param index The index name to be checked against the index pattern given at construction time.
     * @return True if the name matches the pattern, false otherwise.
     */
    public boolean matchesIndexPattern(String index) {
        return indexPatternAutomaton.run(index);
    }

    /**
     * @return A short description of the purpose of this system index.
     */
    public String getDescription() {
        return description;
    }

    /**
     * @return Whether this descriptor supplies mappings that can be maintained by {@link SystemIndexMappingUpdater}.
     */
    public boolean hasMappings() {
        return mappings != null;
    }

    /**
     * @return The concrete index to update, or {@code null} when the mapping target is a write alias or mappings are not supplied.
     */
    public @Nullable String getPrimaryIndex() {
        return primaryIndex;
    }

    /**
     * @return The alias whose write index should be updated, or {@code null} when the mapping target is a concrete index or mappings are not supplied.
     */
    public @Nullable String getWriteAlias() {
        return writeAlias;
    }

    /**
     * @return The desired mapping source, or {@code null} when mappings are not supplied.
     */
    public @Nullable String getMappings() {
        return mappings;
    }

    /**
     * @return The desired mapping schema version, or {@link SystemIndexMappingUpdater#NO_SCHEMA_VERSION} when mappings are not supplied.
     */
    public long getMappingVersion() {
        return mappingVersion;
    }

    @Override
    public String toString() {
        return "SystemIndexDescriptor[pattern=[" + indexPattern + "], description=[" + description + "]]";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof SystemIndexDescriptor)) {
            return false;
        }
        SystemIndexDescriptor other = (SystemIndexDescriptor) obj;
        return indexPattern.equals(other.indexPattern);
    }

    @Override
    public int hashCode() {
        return Objects.hash(indexPattern);
    }

    /**
     * Builder for {@link SystemIndexDescriptor}. Mapping maintenance is optional. A mapping target can be either a concrete primary index
     * or an alias that resolves to a write index.
     */
    @PublicApi(since = "3.9.0")
    public static class Builder {
        private final String indexPattern;
        private final String description;
        private String primaryIndex;
        private String writeAlias;
        private String mappings;

        private Builder(String indexPattern, String description) {
            this.indexPattern = indexPattern;
            this.description = description;
        }

        /**
         * Supplies mappings for a concrete system index.
         *
         * @param primaryIndex The concrete system index to update.
         * @param mappings The desired mappings. They must contain a non-negative {@code _meta.schema_version}.
         * @return This builder.
         */
        public Builder setMappings(String primaryIndex, String mappings) {
            ensureMappingTargetIsUnset();
            this.primaryIndex = Objects.requireNonNull(primaryIndex, "primary index must not be null");
            this.mappings = Objects.requireNonNull(mappings, "mappings must not be null");
            return this;
        }

        /**
         * Supplies mappings for the current write index behind an alias.
         *
         * @param writeAlias The alias whose write index should be updated.
         * @param mappings The desired mappings. They must contain a non-negative {@code _meta.schema_version}.
         * @return This builder.
         */
        public Builder setMappingsForWriteAlias(String writeAlias, String mappings) {
            ensureMappingTargetIsUnset();
            this.writeAlias = Objects.requireNonNull(writeAlias, "write alias must not be null");
            this.mappings = Objects.requireNonNull(mappings, "mappings must not be null");
            return this;
        }

        /**
         * @return A validated system index descriptor.
         */
        public SystemIndexDescriptor build() {
            return new SystemIndexDescriptor(indexPattern, description, primaryIndex, writeAlias, mappings);
        }

        private void ensureMappingTargetIsUnset() {
            if (mappings != null) {
                throw new IllegalStateException("system index mapping target is already configured");
            }
        }
    }

    // TODO: Index settings
    // TODO: getThreadpool()
    // TODO: Upgrade handling for changes that require reindexing
}
