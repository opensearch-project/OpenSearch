/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.compress.CompressedData;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Pure data holder class for alias metadata without dependencies on OpenSearch server packages.
 * This class follows the composition pattern and contains only data fields with getters.
 * <p>
 * AliasMetadataModel stores all essential properties of an alias including:
 * <ul>
 *   <li>alias - the alias name (required)</li>
 *   <li>filter - compressed JSON filter for the alias (optional)</li>
 *   <li>indexRouting - routing value for index operations (optional)</li>
 *   <li>searchRouting - routing value for search operations (optional)</li>
 *   <li>writeIndex - whether this alias is the write index (optional)</li>
 *   <li>isHidden - whether this alias is hidden from wildcard queries (optional)</li>
 * </ul>
 * <p>
 * This class is immutable and thread-safe. Use the {@link Builder} to construct instances.
 *
 * @opensearch.api
 * @since 1.0.0
 */
@PublicApi(since = "1.0.0")
public final class AliasMetadataModel implements Writeable {

    private final String alias;
    private final CompressedData filter;
    private final String indexRouting;
    private final String searchRouting;
    private final Set<String> searchRoutingValues;
    private final Boolean writeIndex;
    private final Boolean isHidden;

    /**
     * Primary constructor for AliasMetadataModel.
     *
     * @param alias the alias name (required, must not be null)
     * @param filter the compressed JSON filter for the alias (optional, may be null)
     * @param indexRouting the routing value for index operations (optional, may be null)
     * @param searchRouting the routing value for search operations (optional, may be null)
     * @param writeIndex whether this alias is the write index (optional, may be null)
     * @param isHidden whether this alias is hidden from wildcard queries (optional, may be null)
     * @throws NullPointerException if alias is null
     */
    public AliasMetadataModel(
        String alias,
        CompressedData filter,
        String indexRouting,
        String searchRouting,
        Boolean writeIndex,
        Boolean isHidden
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
     * @throws NullPointerException if other or newAlias is null
     */
    public AliasMetadataModel(AliasMetadataModel other, String newAlias) {
        this(newAlias, other.filter, other.indexRouting, other.searchRouting, other.writeIndex, other.isHidden);
    }

    /**
     * Deserialization constructor that reads an AliasMetadataModel from a stream.
     *
     * @param in the stream to read from
     * @param compressedDataReader the reader for CompressedData
     * @throws IOException if an I/O error occurs during deserialization
     */
    public <T extends CompressedData> AliasMetadataModel(StreamInput in, Writeable.Reader<T> compressedDataReader) throws IOException {
        this.alias = in.readString();
        this.filter = compressedDataReader.read(in);
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

    // Getters

    /**
     * Returns the alias name.
     *
     * @return the alias name (never null)
     */
    public String alias() {
        return alias;
    }

    /**
     * Returns the alias name (alternative getter for compatibility).
     *
     * @return the alias name (never null)
     */
    public String getAlias() {
        return alias;
    }

    /**
     * Returns the compressed JSON filter for the alias.
     *
     * @return the filter, or null if no filter is set
     */
    public CompressedData filter() {
        return filter;
    }

    /**
     * Returns the compressed JSON filter for the alias (alternative getter for compatibility).
     *
     * @return the filter, or null if no filter is set
     */
    public CompressedData getFilter() {
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
     * Returns the routing value for index operations (alternative getter for compatibility).
     *
     * @return the index routing value, or null if not set
     */
    public String getIndexRouting() {
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
     * Returns the routing value for search operations (alternative getter for compatibility).
     *
     * @return the search routing value, or null if not set
     */
    public String getSearchRouting() {
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
    public Boolean writeIndex() {
        return writeIndex;
    }

    /**
     * Returns whether this alias is the write index (alternative getter for compatibility).
     *
     * @return true if this is the write index, false if explicitly not, null if not set
     */
    public Boolean getWriteIndex() {
        return writeIndex;
    }

    /**
     * Returns whether this alias is hidden from wildcard queries.
     *
     * @return true if hidden, false if not hidden, null if not set
     */
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

    // Serialization (Task 5)

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
        filter.writeTo(out);
        out.writeOptionalString(indexRouting);
        out.writeOptionalString(searchRouting);
        out.writeOptionalBoolean(writeIndex);
        out.writeOptionalBoolean(isHidden);
    }

    // Object methods (Task 6)

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
     * Returns a string representation of this AliasMetadataModel.
     *
     * @return a readable string representation
     */
    @Override
    public String toString() {
        return "AliasMetadataModel{"
            + "alias='"
            + alias
            + '\''
            + ", filter="
            + (filter != null ? "present" : "null")
            + ", indexRouting='"
            + indexRouting
            + '\''
            + ", searchRouting='"
            + searchRouting
            + '\''
            + ", writeIndex="
            + writeIndex
            + ", isHidden="
            + isHidden
            + '}';
    }

    // Builder (Task 7)

    /**
     * Builder for constructing AliasMetadataModel instances.
     * <p>
     * Example usage:
     * <pre>{@code
     * AliasMetadataModel model = new AliasMetadataModel.Builder("my-alias")
     *     .filter(compressedFilter)
     *     .routing("1")
     *     .writeIndex(true)
     *     .isHidden(false)
     *     .build();
     * }</pre>
     */
    public static class Builder {
        private final String alias;
        private CompressedData filter;
        private String indexRouting;
        private String searchRouting;
        private Boolean writeIndex;
        private Boolean isHidden;

        /**
         * Creates a new builder with the specified alias name.
         *
         * @param alias the alias name (required)
         */
        public Builder(String alias) {
            this.alias = alias;
        }

        /**
         * Creates a new builder from an existing model (copy builder).
         * All fields from the model are copied to the builder.
         *
         * @param model the model to copy from
         */
        public Builder(AliasMetadataModel model) {
            this.alias = model.alias;
            this.filter = model.filter;
            this.indexRouting = model.indexRouting;
            this.searchRouting = model.searchRouting;
            this.writeIndex = model.writeIndex;
            this.isHidden = model.isHidden;
        }

        /**
         * Sets the compressed filter for the alias.
         *
         * @param filter the compressed filter
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
        public Builder writeIndex(Boolean writeIndex) {
            this.writeIndex = writeIndex;
            return this;
        }

        /**
         * Sets whether this alias is hidden from wildcard queries.
         *
         * @param isHidden true if hidden, false otherwise
         * @return this builder
         */
        public Builder isHidden(Boolean isHidden) {
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
