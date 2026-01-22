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
import org.opensearch.metadata.index.model.AliasMetadataModel;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Metadata for index aliases
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class AliasMetadata extends AbstractDiffable<AliasMetadata> implements ToXContentFragment {

    private final AliasMetadataModel model;

    private AliasMetadata(AliasMetadataModel model) {
        this.model = model;
    }

    private AliasMetadata(AliasMetadata aliasMetadata, String alias) {
        this.model = new AliasMetadataModel(aliasMetadata.model, alias);
    }

    public String alias() {
        return model.alias();
    }

    public String getAlias() {
        return alias();
    }

    public CompressedXContent filter() {
        if (model.filter() == null) {
            return null;
        }
        return new CompressedXContent(model.filter());
    }

    public CompressedXContent getFilter() {
        return filter();
    }

    public boolean filteringRequired() {
        return model.filteringRequired();
    }

    public String getSearchRouting() {
        return searchRouting();
    }

    public String searchRouting() {
        return model.searchRouting();
    }

    public String getIndexRouting() {
        return indexRouting();
    }

    public String indexRouting() {
        return model.indexRouting();
    }

    public Set<String> searchRoutingValues() {
        return model.searchRoutingValues();
    }

    public Boolean writeIndex() {
        return model.writeIndex();
    }

    @Nullable
    public Boolean isHidden() {
        return model.isHidden();
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

        return Objects.equals(model, that.model);
    }

    @Override
    public int hashCode() {
        return model.hashCode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        model.writeTo(out);
    }

    public AliasMetadata(StreamInput in) throws IOException {
        this.model = new AliasMetadataModel(in);
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

        private final AliasMetadataModel.Builder modelBuilder;

        public Builder(String alias) {
            this.modelBuilder = new AliasMetadataModel.Builder(alias);
        }

        public String alias() {
            return modelBuilder.alias();
        }

        public Builder filter(CompressedXContent filter) {
            if (filter == null) {
                modelBuilder.filter(null);
                return this;
            }
            modelBuilder.filter(filter.compressedData());
            return this;
        }

        public Builder filter(String filter) {
            if (Strings.hasLength(filter) == false) {
                modelBuilder.filter(null);
                return this;
            }
            return filter(XContentHelper.convertToMap(MediaTypeRegistry.xContent(filter).xContent(), filter, true));
        }

        public Builder filter(Map<String, Object> filter) {
            if (filter == null || filter.isEmpty()) {
                modelBuilder.filter(null);
                return this;
            }
            try {
                XContentBuilder builder = XContentFactory.jsonBuilder().map(filter);
                modelBuilder.filter(new CompressedXContent(BytesReference.bytes(builder)).compressedData());
                return this;
            } catch (IOException e) {
                throw new OpenSearchGenerationException("Failed to build json for alias request", e);
            }
        }

        public Builder routing(String routing) {
            modelBuilder.routing(routing);
            return this;
        }

        public Builder indexRouting(String indexRouting) {
            modelBuilder.indexRouting(indexRouting);
            return this;
        }

        public Builder searchRouting(String searchRouting) {
            modelBuilder.searchRouting(searchRouting);
            return this;
        }

        public Builder writeIndex(@Nullable Boolean writeIndex) {
            modelBuilder.writeIndex(writeIndex);
            return this;
        }

        public Builder isHidden(@Nullable Boolean isHidden) {
            modelBuilder.isHidden(isHidden);
            return this;
        }

        public AliasMetadata build() {
            return new AliasMetadata(modelBuilder.build());
        }

        public static void toXContent(AliasMetadata aliasMetadata, XContentBuilder builder, ToXContent.Params params) throws IOException {
            builder.startObject(aliasMetadata.alias());

            boolean binary = params.paramAsBoolean("binary", false);

            if (aliasMetadata.filter() != null) {
                if (binary) {
                    builder.field("filter", aliasMetadata.filter().compressed());
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

            if (aliasMetadata.isHidden() != null) {
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
