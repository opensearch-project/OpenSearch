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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.metadata;

import org.opensearch.OpenSearchGenerationException;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.emptySet;

/**
 * Metadata for index aliases
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AliasMetadata extends AbstractDiffable<AliasMetadata> implements ToXContentFragment {

    private final String alias;

    private final CompressedXContent filter;

    private final String indexRouting;

    private final String searchRouting;

    private final Set<String> searchRoutingValues;

    @Nullable
    private final Boolean writeIndex;

    @Nullable
    private final Boolean isHidden;

    private AliasMetadata(
        String alias,
        CompressedXContent filter,
        String indexRouting,
        String searchRouting,
        Boolean writeIndex,
        @Nullable Boolean isHidden
    ) {
        this.alias = alias;
        this.filter = filter;
        this.indexRouting = indexRouting;
        this.searchRouting = searchRouting;
        if (searchRouting != null) {
            searchRoutingValues = Collections.unmodifiableSet(Sets.newHashSet(Strings.splitStringByCommaToArray(searchRouting)));
        } else {
            searchRoutingValues = emptySet();
        }
        this.writeIndex = writeIndex;
        this.isHidden = isHidden;
    }

    private AliasMetadata(AliasMetadata aliasMetadata, String alias) {
        this(
            alias,
            aliasMetadata.filter(),
            aliasMetadata.indexRouting(),
            aliasMetadata.searchRouting(),
            aliasMetadata.writeIndex(),
            aliasMetadata.isHidden
        );
    }

    public String alias() {
        return alias;
    }

    public String getAlias() {
        return alias();
    }

    public CompressedXContent filter() {
        return filter;
    }

    public CompressedXContent getFilter() {
        return filter();
    }

    public boolean filteringRequired() {
        return filter != null;
    }

    public String getSearchRouting() {
        return searchRouting();
    }

    public String searchRouting() {
        return searchRouting;
    }

    public String getIndexRouting() {
        return indexRouting();
    }

    public String indexRouting() {
        return indexRouting;
    }

    public Set<String> searchRoutingValues() {
        return searchRoutingValues;
    }

    public Boolean writeIndex() {
        return writeIndex;
    }

    @Nullable
    public Boolean isHidden() {
        return isHidden;
    }

    public static Builder builder(String alias) {
        return new Builder(alias);
    }

    public static Builder newAliasMetadataBuilder(String alias) {
        return new Builder(alias);
    }

    /**
     * Creates a new AliasMetadata instance with same content as the given one, but with a different alias name
     */
    public static AliasMetadata newAliasMetadata(AliasMetadata aliasMetadata, String newAlias) {
        return new AliasMetadata(aliasMetadata, newAlias);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final AliasMetadata that = (AliasMetadata) o;

        if (Objects.equals(alias, that.alias) == false) return false;
        if (Objects.equals(filter, that.filter) == false) return false;
        if (Objects.equals(indexRouting, that.indexRouting) == false) return false;
        if (Objects.equals(searchRouting, that.searchRouting) == false) return false;
        if (Objects.equals(writeIndex, that.writeIndex) == false) return false;
        if (Objects.equals(isHidden, that.isHidden) == false) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = alias != null ? alias.hashCode() : 0;
        result = 31 * result + (filter != null ? filter.hashCode() : 0);
        result = 31 * result + (indexRouting != null ? indexRouting.hashCode() : 0);
        result = 31 * result + (searchRouting != null ? searchRouting.hashCode() : 0);
        result = 31 * result + (writeIndex != null ? writeIndex.hashCode() : 0);
        return result;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(alias());
        if (filter() != null) {
            out.writeBoolean(true);
            filter.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (indexRouting() != null) {
            out.writeBoolean(true);
            out.writeString(indexRouting());
        } else {
            out.writeBoolean(false);
        }
        if (searchRouting() != null) {
            out.writeBoolean(true);
            out.writeString(searchRouting());
        } else {
            out.writeBoolean(false);
        }

        out.writeOptionalBoolean(writeIndex());
        out.writeOptionalBoolean(isHidden());
    }

    public AliasMetadata(StreamInput in) throws IOException {
        alias = in.readString();
        if (in.readBoolean()) {
            filter = CompressedXContent.readCompressedString(in);
        } else {
            filter = null;
        }
        if (in.readBoolean()) {
            indexRouting = in.readString();
        } else {
            indexRouting = null;
        }
        if (in.readBoolean()) {
            searchRouting = in.readString();
            searchRoutingValues = Collections.unmodifiableSet(Sets.newHashSet(Strings.splitStringByCommaToArray(searchRouting)));
        } else {
            searchRouting = null;
            searchRoutingValues = emptySet();
        }
        writeIndex = in.readOptionalBoolean();
        isHidden = in.readOptionalBoolean();
    }

    public static Diff<AliasMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(AliasMetadata::new, in);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        AliasMetadata.Builder.toXContent(this, builder, params);
        return builder;
    }

    /**
     * Builder of alias metadata.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private final String alias;

        private CompressedXContent filter;

        private String indexRouting;

        private String searchRouting;

        @Nullable
        private Boolean writeIndex;

        @Nullable
        private Boolean isHidden;

        public Builder(String alias) {
            this.alias = alias;
        }

        public String alias() {
            return alias;
        }

        public Builder filter(CompressedXContent filter) {
            this.filter = filter;
            return this;
        }

        public Builder filter(String filter) {
            if (Strings.hasLength(filter) == false) {
                this.filter = null;
                return this;
            }
            return filter(XContentHelper.convertToMap(MediaTypeRegistry.xContent(filter).xContent(), filter, true));
        }

        public Builder filter(Map<String, Object> filter) {
            if (filter == null || filter.isEmpty()) {
                this.filter = null;
                return this;
            }
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().map(filter);
                this.filter = new CompressedXContent(BytesReference.bytes(builder));
                return this;
            } catch (IOException e) {
                throw new OpenSearchGenerationException("Failed to build json for alias request", e);
            }
        }

        public Builder routing(String routing) {
            this.indexRouting = routing;
            this.searchRouting = routing;
            return this;
        }

        public Builder indexRouting(String indexRouting) {
            this.indexRouting = indexRouting;
            return this;
        }

        public Builder searchRouting(String searchRouting) {
            this.searchRouting = searchRouting;
            return this;
        }

        public Builder writeIndex(@Nullable Boolean writeIndex) {
            this.writeIndex = writeIndex;
            return this;
        }

        public Builder isHidden(@Nullable Boolean isHidden) {
            this.isHidden = isHidden;
            return this;
        }

        public AliasMetadata build() {
            return new AliasMetadata(alias, filter, indexRouting, searchRouting, writeIndex, isHidden);
        }

        public static void toXContent(AliasMetadata aliasMetadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(aliasMetadata.alias());

            boolean binary = params.paramAsBoolean("binary", false);

            if (aliasMetadata.filter() != null) {
                if (binary) {
                    builder.field("filter", aliasMetadata.filter.compressed());
                } else {
                    builder.field("filter", XContentHelper.convertToMap(aliasMetadata.filter().uncompressed(), true).v2());
                }
            }
            if (aliasMetadata.indexRouting() != null) {
                builder.field("index_routing", aliasMetadata.indexRouting());
            }
            if (aliasMetadata.searchRouting() != null) {
                builder.field("search_routing", aliasMetadata.searchRouting());
            }

            if (aliasMetadata.writeIndex() != null) {
                builder.field("is_write_index", aliasMetadata.writeIndex());
            }

            if (aliasMetadata.isHidden != null) {
                builder.field("is_hidden", aliasMetadata.isHidden());
            }

            builder.endObject();
        }

        public static AliasMetadata fromXContent(XContentParser parser) throws IOException {
            Builder builder = new Builder(parser.currentName());

            String currentFieldName = null;
            XContentParser.Token token = parser.nextToken();
            if (token == null) {
                // no data...
                return builder.build();
            }
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("filter".equals(currentFieldName)) {
                        Map<String, Object> filter = parser.mapOrdered();
                        builder.filter(filter);
                    } else {
                        parser.skipChildren();
                    }
                } else if (token == XContentParser.Token.VALUE_EMBEDDED_OBJECT) {
                    if ("filter".equals(currentFieldName)) {
                        builder.filter(new CompressedXContent(parser.binaryValue()));
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("routing".equals(currentFieldName)) {
                        builder.routing(parser.text());
                    } else if ("index_routing".equals(currentFieldName) || "indexRouting".equals(currentFieldName)) {
                        builder.indexRouting(parser.text());
                    } else if ("search_routing".equals(currentFieldName) || "searchRouting".equals(currentFieldName)) {
                        builder.searchRouting(parser.text());
                    } else if ("filter".equals(currentFieldName)) {
                        builder.filter(new CompressedXContent(parser.binaryValue()));
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    parser.skipChildren();
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if ("is_write_index".equals(currentFieldName)) {
                        builder.writeIndex(parser.booleanValue());
                    } else if ("is_hidden".equals(currentFieldName)) {
                        builder.isHidden(parser.booleanValue());
                    }
                }
            }
            return builder.build();
        }
    }
}
