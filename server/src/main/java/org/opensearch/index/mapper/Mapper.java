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

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.similarity.SimilarityProvider;
import org.opensearch.script.ScriptService;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

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

        public BuilderContext(Settings indexSettings, ContentPath contentPath) {
            Objects.requireNonNull(indexSettings, "indexSettings is required");
            this.contentPath = contentPath;
            this.indexSettings = indexSettings;
        }

        public ContentPath path() {
            return this.contentPath;
        }

        public Settings indexSettings() {
            return this.indexSettings;
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
    public abstract static class Builder<T extends Builder> {

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

            public ParserContext(
                Function<String, SimilarityProvider> similarityLookupService,
                MapperService mapperService,
                Function<String, TypeParser> typeParsers,
                Version indexVersionCreated,
                Supplier<QueryShardContext> queryShardContextSupplier,
                DateFormatter dateFormatter,
                ScriptService scriptService
            ) {
                this.similarityLookupService = similarityLookupService;
                this.mapperService = mapperService;
                this.typeParsers = typeParsers;
                this.indexVersionCreated = indexVersionCreated;
                this.queryShardContextSupplier = queryShardContextSupplier;
                this.dateFormatter = dateFormatter;
                this.scriptService = scriptService;
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
                        in.scriptService()
                    );
                }

                @Override
                public boolean isWithinMultiField() {
                    return true;
                }
            }

        }

        Mapper.Builder<?> parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException;
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
}
