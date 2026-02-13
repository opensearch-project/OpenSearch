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
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.compress.CompressedData;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Pure data holder class for alias metadata without dependencies on OpenSearch server packages.
 * This class follows the composition pattern and contains only data fields with getters.
 * <p>
 * AliasMetadataModel stores all essential properties of an alias.
 */
@ExperimentalApi
public final class AliasMetadataModel implements Writeable, ToXContentFragment {

    private static final String FILTER_FIELD = "filter";
    private static final String INDEX_ROUTING_FIELD = "index_routing";
    private static final String SEARCH_ROUTING_FIELD = "search_routing";
    private static final String ROUTING_FIELD = "routing";
    private static final String IS_WRITE_INDEX_FIELD = "is_write_index";
    private static final String IS_HIDDEN_FIELD = "is_hidden";

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
         * Creates a new builder from an existing {@link AliasMetadataModel}.
         *
         * @param model the model to copy from
         */
        public Builder(AliasMetadataModel model) {
            this.alias = model.alias();
            this.filter = model.filter();
            this.indexRouting = model.indexRouting();
            this.searchRouting = model.searchRouting();
            this.writeIndex = model.writeIndex();
            this.isHidden = model.isHidden();
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

        /**
         * Parses an AliasMetadataModel from XContent, handling all input formats including
         * binary embedded objects, camelCase field names, and string-encoded filters.
         * <p>
         * This method matches the parsing behavior of {@code AliasMetadata.Builder.fromXContent()}
         * in the server module, enabling the model layer to fully replace server-side parsing.
         * <p>
         * Expects the parser to be positioned at the alias name field.
         *
         * @param parser the XContent parser
         * @return the parsed AliasMetadataModel
         * @throws IOException if parsing fails
         */
        public static AliasMetadataModel fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                return builder.build();
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if (FILTER_FIELD.equals(currentFieldName)) {
                        Map<String, Object> filterMap = parser.mapOrdered();
                        builder.filter(mapToCompressedData(filterMap));
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                    if (FILTER_FIELD.equals(currentFieldName)) {
                        builder.filter(binaryToCompressedData(parser.binaryValue()));
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (ROUTING_FIELD.equals(currentFieldName)) {
                        builder.routing(parser.text());
                    } else if (INDEX_ROUTING_FIELD.equals(currentFieldName) || "indexRouting".equals(currentFieldName)) {
                        builder.indexRouting(parser.text());
                    } else if (SEARCH_ROUTING_FIELD.equals(currentFieldName) || "searchRouting".equals(currentFieldName)) {
                        builder.searchRouting(parser.text());
                    } else if (FILTER_FIELD.equals(currentFieldName)) {
                        builder.filter(binaryToCompressedData(parser.binaryValue()));
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if (IS_WRITE_INDEX_FIELD.equals(currentFieldName)) {
                        builder.writeIndex(parser.booleanValue());
                    } else if (IS_HIDDEN_FIELD.equals(currentFieldName)) {
                        builder.isHidden(parser.booleanValue());
                    }
                }
            }
            return builder.build();
        }

        /**
         * Writes an AliasMetadataModel to XContent.
         * The alias name is used as the object key, and all non-null fields are written.
         *
         * @param model the AliasMetadataModel to write
         * @param builder the XContent builder
         * @param params the ToXContent params
         * @throws IOException if writing fails
         */
        public static void toXContent(AliasMetadataModel model, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(model.alias());

            boolean binary = params.paramAsBoolean("binary", false);

            if (model.filter() != null) {
                if (binary) {
                    builder.field(FILTER_FIELD, model.filter().compressedBytes());
                } else {
                    byte[] uncompressed = model.filter().uncompressed();
                    try (XContentParser filterParser = JsonXContent.jsonXContent.createParser(null, null, uncompressed)) {
                        filterParser.nextToken();
                        builder.field(FILTER_FIELD, filterParser.mapOrdered());
                    }
                }
            }

            if (model.indexRouting() != null) {
                builder.field(INDEX_ROUTING_FIELD, model.indexRouting());
            }

            if (model.searchRouting() != null) {
                builder.field(SEARCH_ROUTING_FIELD, model.searchRouting());
            }

            if (model.writeIndex() != null) {
                builder.field(IS_WRITE_INDEX_FIELD, model.writeIndex());
            }

            if (model.isHidden() != null) {
                builder.field(IS_HIDDEN_FIELD, model.isHidden());
            }

            builder.endObject();
        }
    }

    /**
     * Parses an AliasMetadataModel from XContent.
     * Delegates to {@link Builder#fromXContent(XContentParser)}.
     *
     * @param parser the XContent parser positioned at the alias name field
     * @return the parsed AliasMetadataModel
     * @throws IOException if parsing fails
     */
    public static AliasMetadataModel fromXContent(XContentParser parser) throws IOException {
        return Builder.fromXContent(parser);
    }

    /**
     * Converts a map to CompressedData by serializing to JSON.
     * The data is stored uncompressed for simplicity.
     */
    private static CompressedData mapToCompressedData(Map<String, Object> map) throws IOException {
        if (map == null || map.isEmpty()) {
            return null;
        }
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.map(map);
        byte[] uncompressed = BytesReference.toBytes(BytesReference.bytes(builder));
        return new CompressedData(uncompressed);
    }

    /**
     * Creates CompressedData from binary bytes (e.g., from VALUE_EMBEDDED_OBJECT or VALUE_STRING binary tokens).
     * Delegates to {@link CompressedData#CompressedData(byte[])} which auto-detects compression.
     *
     * @param bytes the binary data
     * @return a new CompressedData instance, or null if bytes is null or empty
     */
    private static CompressedData binaryToCompressedData(byte[] bytes) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        return new CompressedData(bytes);
    }

    /**
     * Writes this AliasMetadataModel to XContent.
     * Delegates to {@link Builder#toXContent(AliasMetadataModel, XContentBuilder, ToXContent.Params)}.
     *
     * @param builder the XContent builder
     * @param params the ToXContent params
     * @return the XContent builder
     * @throws IOException if writing fails
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        Builder.toXContent(this, builder, params);
        return builder;
    }
}
