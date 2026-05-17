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

package org.opensearch.index.mapper;

import org.apache.lucene.index.LeafReader;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.similarity.SimilarityProvider;
import org.opensearch.script.ScriptService;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.opensearch.index.IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING;

/**
 * The foundation OpenSearch mapper
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class Mapper implements ToXContentFragment, Iterable<Mapper> {

    /**
     * The builder context used in field mappings
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class BuilderContext {
        private final Settings indexSettings;
        private final ContentPath contentPath;
        @Nullable
        private final DataFormatRegistry dataFormatRegistry;
        /**
         * Configured data formats for this index in priority-walk order (primary first, then
         * secondaries by priority ascending). Empty when {@link #dataFormatRegistry} is null
         * or the index does not configure a pluggable data format.
         */
        private final List<DataFormat> configuredFormats;

        public BuilderContext(Settings indexSettings, ContentPath contentPath) {
            this(indexSettings, contentPath, null, List.of());
        }

        public BuilderContext(Settings indexSettings, ContentPath contentPath, @Nullable DataFormatRegistry dataFormatRegistry) {
            this(indexSettings, contentPath, dataFormatRegistry, List.of());
        }

        public BuilderContext(
            Settings indexSettings,
            ContentPath contentPath,
            @Nullable DataFormatRegistry dataFormatRegistry,
            List<DataFormat> configuredFormats
        ) {
            Objects.requireNonNull(indexSettings, "indexSettings is required");
            this.contentPath = contentPath;
            this.indexSettings = indexSettings;
            this.dataFormatRegistry = dataFormatRegistry;
            this.configuredFormats = configuredFormats == null ? List.of() : List.copyOf(configuredFormats);
        }

        public ContentPath path() {
            return this.contentPath;
        }

        public Settings indexSettings() {
            return this.indexSettings;
        }

        /**
         * Returns the data format registry, or {@code null} if pluggable data formats are not enabled.
         */
        @Nullable
        public DataFormatRegistry dataFormatRegistry() {
            return dataFormatRegistry;
        }

        /**
         * Validates capability coverage and assigns the capability map on the given
         * {@link MappedFieldType}. No-op when the {@link DataFormatRegistry} is unavailable
         * (non-composite path).
         *
         * <p>For non-metadata fields with a non-empty {@link MappedFieldType#requestedCapabilities()},
         * walks the {@link #configuredFormats} (primary first, then secondaries by priority
         * ascending — supplied at construction time from
         * {@link DataFormatRegistry#getConfiguredFormats(org.opensearch.index.IndexSettings)})
         * and selects the first format whose {@code supportedFields()} declares support for every
         * requested capability for the field's type. Throws {@link MapperParsingException} when
         * no single configured format can cover the full set. The capability map is set to
         * {@code { coveringFormat -> requestedCapabilities }}.
         *
         * <p>For metadata fields, falls back to
         * {@link DataFormatRegistry#computeCapabilityMap(String, Set, Set)} so the existing
         * metadata routing behavior (per-capability priority winner with defaults) is preserved.
         *
         * <p>For non-metadata fields with an empty requested capability set, sets an empty map
         * without running coverage validation.
         *
         * @param fieldType       the field type to validate and assign
         * @param isMetadataField whether the field is a metadata field; metadata bypasses
         *                        coverage validation
         */
        public void assignCapabilities(MappedFieldType fieldType, boolean isMetadataField) {
            if (dataFormatRegistry == null) {
                return;
            }
            Set<FieldTypeCapabilities.Capability> requested = fieldType.requestedCapabilities();

            if (isMetadataField) {
                fieldType.setCapabilityMap(
                    dataFormatRegistry.computeCapabilityMap(fieldType.typeName(), fieldType.defaultCapabilities(), requested)
                );
                return;
            }

            if (requested.isEmpty()) {
                fieldType.setCapabilityMap(Map.of());
                return;
            }

            if (configuredFormats.isEmpty()) {
                // Registry is available but no formats are configured for this index (e.g.,
                // pluggable data format flag enabled but no plugin name set). Treat as no-op
                // so misconfigurations surface elsewhere rather than as a per-field validation
                // error.
                fieldType.setCapabilityMap(Map.of());
                return;
            }

            for (DataFormat format : configuredFormats) {
                if (formatCovers(format, fieldType.typeName(), requested)) {
                    fieldType.setCapabilityMap(Map.of(format, Set.copyOf(requested)));
                    return;
                }
            }

            throw new MapperParsingException(
                "Field ["
                    + fieldType.name()
                    + "] of type ["
                    + fieldType.typeName()
                    + "] requires capabilities "
                    + requested
                    + " but no single configured data format covers all of them. Configured formats: "
                    + configuredFormats.stream().map(DataFormat::name).collect(Collectors.toList())
            );
        }

        /**
         * Returns whether the given format declares support for every required capability for
         * the given field type name.
         */
        private static boolean formatCovers(DataFormat format, String typeName, Set<FieldTypeCapabilities.Capability> required) {
            return format.supportedFields()
                .stream()
                .filter(ftc -> ftc.fieldType().equals(typeName))
                .findFirst()
                .map(ftc -> ftc.capabilities().containsAll(required))
                .orElse(false);
        }

        public Version indexCreatedVersion() {
            return IndexMetadata.indexCreated(indexSettings);
        }

        public Version indexCreatedVersionOrDefault(@Nullable Version defaultValue) {
            if (defaultValue == null || hasIndexCreated(indexSettings)) {
                return indexCreatedVersion();
            } else {
                return defaultValue;
            }
        }
    }

    /**
     * Base mapper builder
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public abstract static class Builder<T extends Builder> implements MapperBuilderProperties {

        public String name;

        protected T builder;

        protected Builder(String name) {
            this.name = name;
        }

        public String name() {
            return this.name;
        }

        /** Returns a newly built mapper. */
        public abstract Mapper build(BuilderContext context);
    }

    /**
     * Type parser for the mapper
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public interface TypeParser {

        /**
         * Parser context for the type parser
         *
         * @opensearch.api
         */
        @PublicApi(since = "1.0.0")
        class ParserContext {

            private final Function<String, SimilarityProvider> similarityLookupService;

            private final MapperService mapperService;

            private final Function<String, TypeParser> typeParsers;

            private final Version indexVersionCreated;

            private final Supplier<QueryShardContext> queryShardContextSupplier;

            private final DateFormatter dateFormatter;

            private final ScriptService scriptService;

            private final DataFormatRegistry dataFormatRegistry;

            public ParserContext(
                Function<String, SimilarityProvider> similarityLookupService,
                MapperService mapperService,
                Function<String, TypeParser> typeParsers,
                Version indexVersionCreated,
                Supplier<QueryShardContext> queryShardContextSupplier,
                DateFormatter dateFormatter,
                ScriptService scriptService
            ) {
                this(
                    similarityLookupService,
                    mapperService,
                    typeParsers,
                    indexVersionCreated,
                    queryShardContextSupplier,
                    dateFormatter,
                    scriptService,
                    null
                );
            }

            public ParserContext(
                Function<String, SimilarityProvider> similarityLookupService,
                MapperService mapperService,
                Function<String, TypeParser> typeParsers,
                Version indexVersionCreated,
                Supplier<QueryShardContext> queryShardContextSupplier,
                DateFormatter dateFormatter,
                ScriptService scriptService,
                @Nullable DataFormatRegistry dataFormatRegistry
            ) {
                this.similarityLookupService = similarityLookupService;
                this.mapperService = mapperService;
                this.typeParsers = typeParsers;
                this.indexVersionCreated = indexVersionCreated;
                this.queryShardContextSupplier = queryShardContextSupplier;
                this.dateFormatter = dateFormatter;
                this.scriptService = scriptService;
                this.dataFormatRegistry = dataFormatRegistry;
            }

            public IndexAnalyzers getIndexAnalyzers() {
                return mapperService.getIndexAnalyzers();
            }

            public Settings getSettings() {
                return mapperService.getIndexSettings().getSettings();
            }

            public SimilarityProvider getSimilarity(String name) {
                return similarityLookupService.apply(name);
            }

            public MapperService mapperService() {
                return mapperService;
            }

            public TypeParser typeParser(String type) {
                return typeParsers.apply(type);
            }

            public Version indexVersionCreated() {
                return indexVersionCreated;
            }

            public Supplier<QueryShardContext> queryShardContextSupplier() {
                return queryShardContextSupplier;
            }

            /**
             * Gets an optional default date format for date fields that do not have an explicit format set
             * <p>
             * If {@code null}, then date fields will default to {@link DateFieldMapper#DEFAULT_DATE_TIME_FORMATTER}.
             */
            public DateFormatter getDateFormatter() {
                return dateFormatter;
            }

            public boolean isWithinMultiField() {
                return false;
            }

            protected Function<String, TypeParser> typeParsers() {
                return typeParsers;
            }

            protected Function<String, SimilarityProvider> similarityLookupService() {
                return similarityLookupService;
            }

            /**
             * The {@linkplain ScriptService} to compile scripts needed by the {@linkplain Mapper}.
             */
            public ScriptService scriptService() {
                return scriptService;
            }

            /**
             * Returns the DataFormatRegistry, or null if not available.
             * Only non-null when pluggable data format is enabled.
             */
            @Nullable
            public DataFormatRegistry dataFormatRegistry() {
                return dataFormatRegistry;
            }

            public ParserContext createMultiFieldContext(ParserContext in) {
                return new MultiFieldParserContext(in);
            }

            /**
             * Base mutiple field parser context
             *
             * @opensearch.internal
             */
            static class MultiFieldParserContext extends ParserContext {
                MultiFieldParserContext(ParserContext in) {
                    super(
                        in.similarityLookupService(),
                        in.mapperService(),
                        in.typeParsers(),
                        in.indexVersionCreated(),
                        in.queryShardContextSupplier(),
                        in.getDateFormatter(),
                        in.scriptService(),
                        in.dataFormatRegistry()
                    );
                }

                @Override
                public boolean isWithinMultiField() {
                    return true;
                }
            }

        }

        Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException;

        default Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext, ObjectMapper.Builder objBuilder)
            throws MapperParsingException {
            throw new UnsupportedOperationException("should not be invoked");
        }
    }

    private final String simpleName;

    public Mapper(String simpleName) {
        Objects.requireNonNull(simpleName);
        this.simpleName = simpleName;
    }

    /** Returns the simple name, which identifies this mapper against other mappers at the same level in the mappers hierarchy
     * TODO: make this protected once Mapper and FieldMapper are merged together */
    public final String simpleName() {
        return simpleName;
    }

    /** Returns the canonical name which uniquely identifies the mapper against other mappers in a type. */
    public abstract String name();

    /**
     * Returns a name representing the type of this mapper.
     */
    public abstract String typeName();

    /** Return the merge of {@code mergeWith} into this.
     *  Both {@code this} and {@code mergeWith} will be left unmodified. */
    public abstract Mapper merge(Mapper mergeWith);

    /**
     * Validate any cross-field references made by this mapper
     * @param mappers a {@link MappingLookup} that can produce references to other mappers
     */
    public abstract void validate(MappingLookup mappers);

    /**
     * Check if settings have IndexMetadata.SETTING_INDEX_VERSION_CREATED setting.
     * @param settings settings
     * @return "true" if settings have IndexMetadata.SETTING_INDEX_VERSION_CREATED setting, "false" otherwise
     */
    protected static boolean hasIndexCreated(Settings settings) {
        return settings.hasValue(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey());
    }

    /**
     * Checks if the optimised index feature is enabled for the given settings.
     * Requires both the {@link FeatureFlags#PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG} feature flag
     *
     * @param settings the index settings to check
     * @return {@code true} if the pluggable dataformat feature flag and the optimised index setting are both enabled
     */
    public static boolean isPluggableDataFormatEnabled(Settings settings) {
        return FeatureFlags.isEnabled(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
            && PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(settings);
    }

    /**
     * Method to determine, if it is possible to derive source for this field using field mapping parameters
     */
    public void canDeriveSource() {
        throw new UnsupportedOperationException("Derived source field is not supported for [" + name() + "] field");
    }

    /**
     * Method used for deriving source and building it to XContentBuilder object
     * @param builder - builder to store the derived source filed
     * @param leafReader - leafReader to read data from
     * @param docId - docId for which we want to derive the source
     */
    public void deriveSource(XContentBuilder builder, LeafReader leafReader, int docId) throws IOException {
        throw new UnsupportedOperationException("Derived source field is not supported for [" + name() + "] field");
    }
}
