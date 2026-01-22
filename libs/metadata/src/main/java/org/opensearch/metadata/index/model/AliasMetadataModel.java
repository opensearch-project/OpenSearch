/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.metadata.compress.CompressedData;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Pure data holder class for alias metadata without dependencies on OpenSearch server packages.
 * This class follows the composition pattern and contains only data fields with getters.
 * <p>
 * AliasMetadataModel stores all essential properties of an alias.
 */
@ExperimentalApi
public final class AliasMetadataModel implements Writeable {

    private final String alias;
    private final CompressedData filter;
    private final String indexRouting;
    private final String searchRouting;
    private final Set<String> searchRoutingValues;
    @Nullable
    private final Boolean writeIndex;
    @Nullable
    private final Boolean isHidden;

    /**
     * Primary constructor for AliasMetadataModel.
     *
     * @param alias the alias name (must not be null)
     * @param filter the filter for the alias
     * @param indexRouting the routing value for index operations
     * @param searchRouting the routing value for search operations
     * @param writeIndex whether this alias is the write index
     * @param isHidden whether this alias is hidden from wildcard queries
     * @throws NullPointerException if alias is null
     */
    public AliasMetadataModel(
        String alias,
        CompressedData filter,
        String indexRouting,
        String searchRouting,
        @Nullable Boolean writeIndex,
        @Nullable Boolean isHidden
    ) {
        this.alias = Objects.requireNonNull(alias, "alias must not be null");
        this.filter = filter;
        this.indexRouting = indexRouting;
        this.searchRouting = searchRouting;
        this.searchRoutingValues = parseSearchRoutingValues(searchRouting);
        this.writeIndex = writeIndex;
        this.isHidden = isHidden;
    }

    /**
     * Copy constructor that creates a new AliasMetadataModel with a different alias name.
     * All other fields are copied from the source model.
     *
     * @param other the source AliasMetadataModel to copy from
     * @param newAlias the new alias name
     */
    public AliasMetadataModel(AliasMetadataModel other, String newAlias) {
        this(newAlias, other.filter, other.indexRouting, other.searchRouting, other.writeIndex, other.isHidden);
    }

    /**
     * Deserialization constructor that reads an AliasMetadataModel from a stream.
     *
     * @param in the stream to read from
     */
    public AliasMetadataModel(StreamInput in) throws IOException {
        this.alias = in.readString();
        if (in.readBoolean()) {
            filter = new CompressedData(in);
        } else {
            filter = null;
        }
        this.indexRouting = in.readOptionalString();
        this.searchRouting = in.readOptionalString();
        this.searchRoutingValues = parseSearchRoutingValues(searchRouting);
        this.writeIndex = in.readOptionalBoolean();
        this.isHidden = in.readOptionalBoolean();
    }

    /**
     * Parses the searchRouting string into a set of routing values.
     * If searchRouting is null, returns an empty set.
     *
     * @param searchRouting the comma-separated search routing string
     * @return an unmodifiable set of routing values
     */
    private static Set<String> parseSearchRoutingValues(String searchRouting) {
        if (searchRouting == null) {
            return Collections.emptySet();
        }
        return Collections.unmodifiableSet(Sets.newHashSet(Strings.splitStringByCommaToArray(searchRouting)));
    }

    /**
     * Returns the alias name.
     *
     * @return the alias name (never null)
     */
    public String alias() {
        return alias;
    }

    /**
     * Returns the filter for the alias.
     *
     * @return the filter, or null if no filter is set
     */
    public CompressedData filter() {
        return filter;
    }

    /**
     * Returns the routing value for index operations.
     *
     * @return the index routing value, or null if not set
     */
    public String indexRouting() {
        return indexRouting;
    }

    /**
     * Returns the routing value for search operations.
     *
     * @return the search routing value, or null if not set
     */
    public String searchRouting() {
        return searchRouting;
    }

    /**
     * Returns the parsed set of search routing values.
     * This is derived from the searchRouting string by splitting on commas.
     *
     * @return an unmodifiable set of search routing values (never null, may be empty)
     */
    public Set<String> searchRoutingValues() {
        return searchRoutingValues;
    }

    /**
     * Returns whether this alias is the write index.
     *
     * @return true if this is the write index, false if explicitly not, null if not set
     */
    @Nullable
    public Boolean writeIndex() {
        return writeIndex;
    }

    /**
     * Returns whether this alias is hidden from wildcard queries.
     *
     * @return true if hidden, false if not hidden, null if not set
     */
    @Nullable
    public Boolean isHidden() {
        return isHidden;
    }

    /**
     * Returns whether filtering is required for this alias.
     * Filtering is required if a filter is set.
     *
     * @return true if a filter is set, false otherwise
     */
    public boolean filteringRequired() {
        return filter != null;
    }

    /**
     * Writes this AliasMetadataModel to a stream.
     * The serialization format is compatible with AliasMetadata.
     *
     * @param out the stream to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(alias);
        if (filter != null) {
            out.writeBoolean(true);
            filter.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(indexRouting);
        out.writeOptionalString(searchRouting);
        out.writeOptionalBoolean(writeIndex);
        out.writeOptionalBoolean(isHidden);
    }

    /**
     * Compares this AliasMetadataModel with another object for equality.
     * Two models are equal if all their fields are equal.
     *
     * @param o the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AliasMetadataModel that = (AliasMetadataModel) o;

        return Objects.equals(alias, that.alias)
            && Objects.equals(filter, that.filter)
            && Objects.equals(indexRouting, that.indexRouting)
            && Objects.equals(searchRouting, that.searchRouting)
            && Objects.equals(writeIndex, that.writeIndex)
            && Objects.equals(isHidden, that.isHidden);
    }

    /**
     * Returns the hash code for this AliasMetadataModel.
     * The hash code is computed from all fields.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hash(alias, filter, indexRouting, searchRouting, writeIndex, isHidden);
    }

    /**
     * Builder for constructing AliasMetadataModel instances.
     */
    @ExperimentalApi
    public static class Builder {
        private final String alias;
        private CompressedData filter;
        private String indexRouting;
        private String searchRouting;
        @Nullable
        private Boolean writeIndex;
        @Nullable
        private Boolean isHidden;

        /**
         * Creates a new builder with the specified alias name.
         *
         * @param alias the alias name (required, must not be null)
         * @throws NullPointerException if alias is null
         */
        public Builder(String alias) {
            this.alias = Objects.requireNonNull(alias, "alias must not be null");
        }

        /**
         * Returns the alias name from builder.
         *
         * @return the alias name
         */
        public String alias() {
            return alias;
        }

        /**
         * Sets the filter for the alias.
         *
         * @param filter the filter
         * @return this builder
         */
        public Builder filter(CompressedData filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Sets the routing value for index operations.
         *
         * @param indexRouting the index routing value
         * @return this builder
         */
        public Builder indexRouting(String indexRouting) {
            this.indexRouting = indexRouting;
            return this;
        }

        /**
         * Sets the routing value for search operations.
         *
         * @param searchRouting the search routing value
         * @return this builder
         */
        public Builder searchRouting(String searchRouting) {
            this.searchRouting = searchRouting;
            return this;
        }

        /**
         * Sets both index and search routing to the same value.
         *
         * @param routing the routing value for both index and search
         * @return this builder
         */
        public Builder routing(String routing) {
            this.indexRouting = routing;
            this.searchRouting = routing;
            return this;
        }

        /**
         * Sets whether this alias is the write index.
         *
         * @param writeIndex true if this is the write index, false otherwise
         * @return this builder
         */
        public Builder writeIndex(@Nullable Boolean writeIndex) {
            this.writeIndex = writeIndex;
            return this;
        }

        /**
         * Sets whether this alias is hidden from wildcard queries.
         *
         * @param isHidden true if hidden, false otherwise
         * @return this builder
         */
        public Builder isHidden(@Nullable Boolean isHidden) {
            this.isHidden = isHidden;
            return this;
        }

        /**
         * Builds the AliasMetadataModel instance.
         *
         * @return a new AliasMetadataModel
         */
        public AliasMetadataModel build() {
            return new AliasMetadataModel(alias, filter, indexRouting, searchRouting, writeIndex, isHidden);
        }
    }
}
