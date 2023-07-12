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

import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.AbstractDiffable;
import org.opensearch.cluster.Diff;
import org.opensearch.common.Nullable;
import org.opensearch.common.Strings;
import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Metadata for Index Templates
 *
 * @opensearch.internal
 */
public class IndexTemplateMetadata extends AbstractDiffable<IndexTemplateMetadata> {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(IndexTemplateMetadata.class);

    private final String name;

    private final int order;

    /**
     * The version is an arbitrary number managed by the user so that they can easily and quickly verify the existence of a given template.
     * Expected usage:
     * <pre><code>
     * PUT /_template/my_template
     * {
     *   "index_patterns": ["my_index-*"],
     *   "mappings": { ... },
     *   "version": 1
     * }
     * </code></pre>
     * Then, some process from the user can occasionally verify that the template exists with the appropriate version without having to
     * check the template's content:
     * <pre><code>
     * GET /_template/my_template?filter_path=*.version
     * </code></pre>
     */
    @Nullable
    private final Integer version;

    private final List<String> patterns;

    private final Settings settings;

    // the mapping source should always include the type as top level
    private final Map<String, CompressedXContent> mappings;

    private final Map<String, AliasMetadata> aliases;

    public IndexTemplateMetadata(
        String name,
        int order,
        Integer version,
        List<String> patterns,
        Settings settings,
        final Map<String, CompressedXContent> mappings,
        final Map<String, AliasMetadata> aliases
    ) {
        if (patterns == null || patterns.isEmpty()) {
            throw new IllegalArgumentException("Index patterns must not be null or empty; got " + patterns);
        }
        this.name = name;
        this.order = order;
        this.version = version;
        this.patterns = patterns;
        this.settings = settings;
        this.mappings = Collections.unmodifiableMap(mappings);
        if (this.mappings.size() > 1) {
            deprecationLogger.deprecate(
                "index-templates",
                "Index template {} contains multiple typed mappings; templates in 8x will only support a single mapping",
                name
            );
        }
        this.aliases = Collections.unmodifiableMap(aliases);
    }

    public String name() {
        return this.name;
    }

    public int order() {
        return this.order;
    }

    public int getOrder() {
        return order();
    }

    @Nullable
    public Integer getVersion() {
        return version();
    }

    @Nullable
    public Integer version() {
        return version;
    }

    public String getName() {
        return this.name;
    }

    public List<String> patterns() {
        return this.patterns;
    }

    public Settings settings() {
        return this.settings;
    }

    public CompressedXContent mappings() {
        if (this.mappings.isEmpty()) {
            return null;
        }
        return this.mappings.values().iterator().next();
    }

    public CompressedXContent getMappings() {
        return this.mappings();
    }

    public Map<String, AliasMetadata> aliases() {
        return this.aliases;
    }

    public Map<String, AliasMetadata> getAliases() {
        return this.aliases;
    }

    public static Builder builder(String name) {
        return new Builder(name);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexTemplateMetadata that = (IndexTemplateMetadata) o;

        if (order != that.order) return false;
        if (!mappings.equals(that.mappings)) return false;
        if (!name.equals(that.name)) return false;
        if (!settings.equals(that.settings)) return false;
        if (!patterns.equals(that.patterns)) return false;

        return Objects.equals(aliases, that.aliases) && Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + order;
        result = 31 * result + Objects.hashCode(version);
        result = 31 * result + patterns.hashCode();
        result = 31 * result + settings.hashCode();
        result = 31 * result + mappings.hashCode();
        result = 31 * result + aliases.hashCode();
        return result;
    }

    public static IndexTemplateMetadata readFrom(StreamInput in) throws IOException {
        Builder builder = new Builder(in.readString());
        builder.order(in.readInt());
        builder.patterns(in.readStringList());
        builder.settings(Settings.readSettingsFromStream(in));
        int mappingsSize = in.readVInt();
        for (int i = 0; i < mappingsSize; i++) {
            builder.putMapping(in.readString(), CompressedXContent.readCompressedString(in));
        }
        int aliasesSize = in.readVInt();
        for (int i = 0; i < aliasesSize; i++) {
            AliasMetadata aliasMd = new AliasMetadata(in);
            builder.putAlias(aliasMd);
        }
        builder.version(in.readOptionalVInt());
        return builder.build();
    }

    public static Diff<IndexTemplateMetadata> readDiffFrom(StreamInput in) throws IOException {
        return readDiffFrom(IndexTemplateMetadata::readFrom, in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeInt(order);
        out.writeStringCollection(patterns);
        Settings.writeSettingsToStream(settings, out);
        out.writeVInt(mappings.size());
        for (final Map.Entry<String, CompressedXContent> cursor : mappings.entrySet()) {
            out.writeString(cursor.getKey());
            cursor.getValue().writeTo(out);
        }
        out.writeVInt(aliases.size());
        for (final AliasMetadata cursor : aliases.values()) {
            cursor.writeTo(out);
        }
        out.writeOptionalVInt(version);
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            Builder.toXContentWithTypes(this, builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Builder of index template metadata.
     *
     * @opensearch.internal
     */
    public static class Builder {

        private static final Set<String> VALID_FIELDS = Sets.newHashSet(
            "order",
            "mappings",
            "settings",
            "index_patterns",
            "aliases",
            "version"
        );

        private String name;

        private int order;

        private Integer version;

        private List<String> indexPatterns;

        private Settings settings = Settings.Builder.EMPTY_SETTINGS;

        private final Map<String, CompressedXContent> mappings;

        private final Map<String, AliasMetadata> aliases;

        public Builder(String name) {
            this.name = name;
            mappings = new HashMap<>();
            aliases = new HashMap<>();
        }

        public Builder(IndexTemplateMetadata indexTemplateMetadata) {
            this.name = indexTemplateMetadata.name();
            order(indexTemplateMetadata.order());
            version(indexTemplateMetadata.version());
            patterns(indexTemplateMetadata.patterns());
            settings(indexTemplateMetadata.settings());

            mappings = new HashMap<>(indexTemplateMetadata.mappings);
            aliases = new HashMap<>(indexTemplateMetadata.aliases());
        }

        public Builder order(int order) {
            this.order = order;
            return this;
        }

        public Builder version(Integer version) {
            this.version = version;
            return this;
        }

        public Builder patterns(List<String> indexPatterns) {
            this.indexPatterns = indexPatterns;
            return this;
        }

        public Builder settings(Settings.Builder settings) {
            this.settings = settings.build();
            return this;
        }

        public Builder settings(Settings settings) {
            this.settings = settings;
            return this;
        }

        public Builder putMapping(String mappingType, CompressedXContent mappingSource) {
            mappings.put(mappingType, mappingSource);
            return this;
        }

        public Builder putMapping(String mappingType, String mappingSource) throws IOException {
            mappings.put(mappingType, new CompressedXContent(mappingSource));
            return this;
        }

        public Builder putAlias(AliasMetadata aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata);
            return this;
        }

        public Builder putAlias(AliasMetadata.Builder aliasMetadata) {
            aliases.put(aliasMetadata.alias(), aliasMetadata.build());
            return this;
        }

        public IndexTemplateMetadata build() {
            return new IndexTemplateMetadata(name, order, version, indexPatterns, settings, mappings, aliases);
        }

        /**
         * Serializes the template to xContent, using the legacy format where the mappings are
         * nested under the type name.
         *
         * This method is used for serializing templates before storing them in the cluster metadata,
         * and also in the REST layer when returning a deprecated typed response.
         */
        public static void toXContentWithTypes(
            IndexTemplateMetadata indexTemplateMetadata,
            XContentBuilder builder,
            ToXContent.Params params
        ) throws IOException {
            builder.startObject(indexTemplateMetadata.name());
            toInnerXContent(indexTemplateMetadata, builder, params, true);
            builder.endObject();
        }

        /**
         * Serializes the template to xContent, making sure not to nest mappings under the
         * type name.
         *
         * Note that this method should currently only be used for creating REST responses,
         * and not when directly updating stored templates. Index templates are still stored
         * in the old, typed format, and have yet to be migrated to be typeless.
         */
        public static void toXContent(IndexTemplateMetadata indexTemplateMetadata, XContentBuilder builder, ToXContent.Params params)
            throws IOException {
            builder.startObject(indexTemplateMetadata.name());
            toInnerXContent(indexTemplateMetadata, builder, params, false);
            builder.endObject();
        }

        static void toInnerXContentWithTypes(IndexTemplateMetadata indexTemplateMetadata, XContentBuilder builder, ToXContent.Params params)
            throws IOException {
            toInnerXContent(indexTemplateMetadata, builder, params, true);
        }

        private static void toInnerXContent(
            IndexTemplateMetadata indexTemplateMetadata,
            XContentBuilder builder,
            ToXContent.Params params,
            boolean includeTypeName
        ) throws IOException {
            builder.field("order", indexTemplateMetadata.order());
            if (indexTemplateMetadata.version() != null) {
                builder.field("version", indexTemplateMetadata.version());
            }
            builder.field("index_patterns", indexTemplateMetadata.patterns());

            builder.startObject("settings");
            indexTemplateMetadata.settings().toXContent(builder, params);
            builder.endObject();

            includeTypeName &= (params.paramAsBoolean("reduce_mappings", false) == false);
            CompressedXContent m = indexTemplateMetadata.mappings();
            if (m != null) {
                Map<String, Object> documentMapping = XContentHelper.convertToMap(m.uncompressed(), true).v2();
                if (includeTypeName == false) {
                    documentMapping = reduceMapping(documentMapping);
                } else {
                    documentMapping = reduceEmptyMapping(documentMapping);
                }
                builder.field("mappings");
                builder.map(documentMapping);
            } else {
                builder.startObject("mappings").endObject();
            }

            builder.startObject("aliases");
            for (final AliasMetadata cursor : indexTemplateMetadata.aliases().values()) {
                AliasMetadata.Builder.toXContent(cursor, builder, params);
            }
            builder.endObject();
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> reduceEmptyMapping(Map<String, Object> mapping) {
            if (mapping.keySet().size() == 1
                && mapping.containsKey(MapperService.SINGLE_MAPPING_NAME)
                && ((Map<String, Object>) mapping.get(MapperService.SINGLE_MAPPING_NAME)).size() == 0) {
                return (Map<String, Object>) mapping.values().iterator().next();
            } else {
                return mapping;
            }
        }

        @SuppressWarnings("unchecked")
        private static Map<String, Object> reduceMapping(Map<String, Object> mapping) {
            assert mapping.keySet().size() == 1 : mapping.keySet();
            return (Map<String, Object>) mapping.values().iterator().next();
        }

        public static IndexTemplateMetadata fromXContent(XContentParser parser, String templateName) throws IOException {
            Builder builder = new Builder(templateName);

            String currentFieldName = skipTemplateName(parser);
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    if ("settings".equals(currentFieldName)) {
                        Settings.Builder templateSettingsBuilder = Settings.builder();
                        templateSettingsBuilder.put(Settings.fromXContent(parser));
                        templateSettingsBuilder.normalizePrefix(IndexMetadata.INDEX_SETTING_PREFIX);
                        builder.settings(templateSettingsBuilder.build());
                    } else if ("mappings".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (token == XContentParser.Token.FIELD_NAME) {
                                currentFieldName = parser.currentName();
                            } else if (token == XContentParser.Token.START_OBJECT) {
                                String mappingType = currentFieldName;
                                Map<String, Object> mappingSource = MapBuilder.<String, Object>newMapBuilder()
                                    .put(mappingType, parser.mapOrdered())
                                    .map();
                                builder.putMapping(mappingType, Strings.toString(XContentFactory.jsonBuilder().map(mappingSource)));
                            }
                        }
                    } else if ("aliases".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            builder.putAlias(AliasMetadata.Builder.fromXContent(parser));
                        }
                    } else {
                        throw new OpenSearchParseException("unknown key [{}] for index template", currentFieldName);
                    }
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if ("mappings".equals(currentFieldName)) {
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            Map<String, Object> mapping = parser.mapOrdered();
                            if (mapping.size() == 1) {
                                String mappingType = mapping.keySet().iterator().next();
                                String mappingSource = Strings.toString(XContentFactory.jsonBuilder().map(mapping));

                                if (mappingSource == null) {
                                    // crap, no mapping source, warn?
                                } else {
                                    builder.putMapping(mappingType, mappingSource);
                                }
                            }
                        }
                    } else if ("index_patterns".equals(currentFieldName)) {
                        List<String> index_patterns = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            index_patterns.add(parser.text());
                        }
                        builder.patterns(index_patterns);
                    }
                } else if (token.isValue()) {
                    if ("order".equals(currentFieldName)) {
                        builder.order(parser.intValue());
                    } else if ("version".equals(currentFieldName)) {
                        builder.version(parser.intValue());
                    }
                }
            }
            return builder.build();
        }

        private static String skipTemplateName(XContentParser parser) throws IOException {
            XContentParser.Token token = parser.nextToken();
            if (token == XContentParser.Token.START_OBJECT) {
                token = parser.nextToken();
                if (token == XContentParser.Token.FIELD_NAME) {
                    String currentFieldName = parser.currentName();
                    if (VALID_FIELDS.contains(currentFieldName)) {
                        return currentFieldName;
                    } else {
                        // we just hit the template name, which should be ignored and we move on
                        parser.nextToken();
                    }
                }
            }

            return null;
        }
    }

}
