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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentContraints;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.Assertions;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.AbstractIndexComponent;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.index.analysis.CharFilterFactory;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.analysis.NamedAnalyzer;
import org.opensearch.index.analysis.ReloadableCustomAnalyzer;
import org.opensearch.index.analysis.TokenFilterFactory;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.index.mapper.Mapper.BuilderContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.InvalidTypeNameException;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.script.ScriptService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * The core field mapping service
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class MapperService extends AbstractIndexComponent implements Closeable {

    /**
     * The reason why a mapping is being merged.
     *
     * @opensearch.internal
     */
    @PublicApi(since = "1.0.0")
    public enum MergeReason {
        /**
         * Pre-flight check before sending a mapping update to the cluster-manager
         */
        MAPPING_UPDATE_PREFLIGHT,
        /**
         * Create or update a mapping.
         */
        MAPPING_UPDATE,
        /**
         * Merge mappings from a composable index template.
         */
        INDEX_TEMPLATE,
        /**
         * Recovery of an existing mapping, for instance because of a restart,
         * if a shard was moved to a different node or for administrative
         * purposes.
         */
        MAPPING_RECOVERY;
    }

    public static final String SINGLE_MAPPING_NAME = "_doc";
    public static final Setting<Long> INDEX_MAPPING_NESTED_FIELDS_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.nested_fields.limit",
        50L,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    // maximum allowed number of nested json objects across all fields in a single document
    public static final Setting<Long> INDEX_MAPPING_NESTED_DOCS_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.nested_objects.limit",
        10000L,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Long> INDEX_MAPPING_TOTAL_FIELDS_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.total_fields.limit",
        1000L,
        0,
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Long> INDEX_MAPPING_DEPTH_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.depth.limit",
        20L,
        1,
        Long.MAX_VALUE,
        limit -> {
            // Make sure XContent constraints are not exceeded (otherwise content processing will fail)
            if (limit > XContentContraints.DEFAULT_MAX_DEPTH) {
                throw new IllegalArgumentException(
                    "The provided value "
                        + limit
                        + " of the index setting 'index.mapping.depth.limit' exceeds per-JVM configured limit of "
                        + XContentContraints.DEFAULT_MAX_DEPTH
                        + ". Please change the setting value or increase per-JVM limit "
                        + "using '"
                        + XContentContraints.DEFAULT_MAX_DEPTH_PROPERTY
                        + "' system property."
                );
            }
        },
        Property.Dynamic,
        Property.IndexScope
    );
    public static final Setting<Long> INDEX_MAPPING_FIELD_NAME_LENGTH_LIMIT_SETTING = Setting.longSetting(
        "index.mapping.field_name_length.limit",
        50000,
        1L,
        Long.MAX_VALUE,
        limit -> {
            // Make sure XContent constraints are not exceeded (otherwise content processing will fail)
            if (limit > XContentContraints.DEFAULT_MAX_NAME_LEN) {
                throw new IllegalArgumentException(
                    "The provided value "
                        + limit
                        + " of the index setting 'index.mapping.field_name_length.limit' exceeds per-JVM configured limit of "
                        + XContentContraints.DEFAULT_MAX_NAME_LEN
                        + ". Please change the setting value or increase per-JVM limit "
                        + "using '"
                        + XContentContraints.DEFAULT_MAX_NAME_LEN_PROPERTY
                        + "' system property."
                );
            }
        },
        Property.Dynamic,
        Property.IndexScope
    );
    public static final boolean INDEX_MAPPER_DYNAMIC_DEFAULT = true;
    @Deprecated
    public static final Setting<Boolean> INDEX_MAPPER_DYNAMIC_SETTING = Setting.boolSetting(
        "index.mapper.dynamic",
        INDEX_MAPPER_DYNAMIC_DEFAULT,
        Property.Dynamic,
        Property.IndexScope,
        Property.Deprecated
    );
    // Deprecated set of meta-fields, for checking if a field is meta, use an instance method isMetadataField instead
    @Deprecated
    public static final Set<String> META_FIELDS_BEFORE_7DOT8 = Collections.unmodifiableSet(
        new HashSet<>(Arrays.asList("_id", IgnoredFieldMapper.NAME, "_index", "_routing", "_size", "_timestamp", "_ttl", "_type"))
    );

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(MapperService.class);

    private final IndexAnalyzers indexAnalyzers;

    private volatile DocumentMapper mapper;

    private final DocumentMapperParser documentParser;
    private final Version indexVersionCreated;

    private final MapperAnalyzerWrapper indexAnalyzer;
    private final MapperAnalyzerWrapper searchAnalyzer;
    private final MapperAnalyzerWrapper searchQuoteAnalyzer;

    private volatile Map<String, MappedFieldType> unmappedFieldTypes = emptyMap();

    final MapperRegistry mapperRegistry;

    private final BooleanSupplier idFieldDataEnabled;

    private volatile Set<CompositeMappedFieldType> compositeMappedFieldTypes;
    private volatile Set<String> fieldsPartOfCompositeMappings;

    public MapperService(
        IndexSettings indexSettings,
        IndexAnalyzers indexAnalyzers,
        NamedXContentRegistry xContentRegistry,
        SimilarityService similarityService,
        MapperRegistry mapperRegistry,
        Supplier<QueryShardContext> queryShardContextSupplier,
        BooleanSupplier idFieldDataEnabled,
        ScriptService scriptService
    ) {
        super(indexSettings);

        this.indexVersionCreated = indexSettings.getIndexVersionCreated();
        this.indexAnalyzers = indexAnalyzers;
        this.documentParser = new DocumentMapperParser(
            indexSettings,
            this,
            xContentRegistry,
            similarityService,
            mapperRegistry,
            queryShardContextSupplier,
            scriptService
        );
        this.indexAnalyzer = new MapperAnalyzerWrapper(indexAnalyzers.getDefaultIndexAnalyzer(), MappedFieldType::indexAnalyzer);
        this.searchAnalyzer = new MapperAnalyzerWrapper(
            indexAnalyzers.getDefaultSearchAnalyzer(),
            p -> p.getTextSearchInfo().getSearchAnalyzer()
        );
        this.searchQuoteAnalyzer = new MapperAnalyzerWrapper(
            indexAnalyzers.getDefaultSearchQuoteAnalyzer(),
            p -> p.getTextSearchInfo().getSearchQuoteAnalyzer()
        );
        this.mapperRegistry = mapperRegistry;
        this.idFieldDataEnabled = idFieldDataEnabled;

        if (INDEX_MAPPER_DYNAMIC_SETTING.exists(indexSettings.getSettings())) {
            deprecationLogger.deprecate(
                index().getName() + INDEX_MAPPER_DYNAMIC_SETTING.getKey(),
                "Index [{}] has setting [{}] that is not supported in OpenSearch, its value will be ignored.",
                index().getName(),
                INDEX_MAPPER_DYNAMIC_SETTING.getKey()
            );
        }
    }

    public boolean hasNested() {
        return this.mapper != null && this.mapper.hasNestedObjects();
    }

    public IndexAnalyzers getIndexAnalyzers() {
        return this.indexAnalyzers;
    }

    public NamedAnalyzer getNamedAnalyzer(String analyzerName) {
        return this.indexAnalyzers.get(analyzerName);
    }

    public DocumentMapperParser documentMapperParser() {
        return this.documentParser;
    }

    /**
     * Parses the mappings (formatted as JSON) into a map
     */
    public static Map<String, Object> parseMapping(NamedXContentRegistry xContentRegistry, String mappingSource) throws IOException {
        try (
            XContentParser parser = MediaTypeRegistry.JSON.xContent()
                .createParser(xContentRegistry, LoggingDeprecationHandler.INSTANCE, mappingSource)
        ) {
            return parser.map();
        }
    }

    /**
     * Update mapping by only merging the metadata that is different between received and stored entries
     */
    public boolean updateMapping(final IndexMetadata currentIndexMetadata, final IndexMetadata newIndexMetadata) throws IOException {
        assert newIndexMetadata.getIndex().equals(index()) : "index mismatch: expected "
            + index()
            + " but was "
            + newIndexMetadata.getIndex();

        if (currentIndexMetadata != null && currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
            assertMappingVersion(currentIndexMetadata, newIndexMetadata, Collections.emptyMap());
            return false;
        }

        // go over and add the relevant mappings (or update them)
        Set<String> existingMappers = new HashSet<>();
        if (mapper != null) {
            existingMappers.add(mapper.type());
        }
        final Map<String, DocumentMapper> updatedEntries;
        try {
            // only update entries if needed
            updatedEntries = internalMerge(newIndexMetadata, MergeReason.MAPPING_RECOVERY);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("[{}] failed to apply mappings", index()), e);
            throw e;
        }

        boolean requireRefresh = false;

        assertMappingVersion(currentIndexMetadata, newIndexMetadata, updatedEntries);

        for (DocumentMapper documentMapper : updatedEntries.values()) {
            String mappingType = documentMapper.type();
            MappingMetadata mappingMetadata = newIndexMetadata.mapping();
            assert mappingType.equals(mappingMetadata.type());
            CompressedXContent incomingMappingSource = mappingMetadata.source();

            String op = existingMappers.contains(mappingType) ? "updated" : "added";
            if (logger.isDebugEnabled() && incomingMappingSource.compressed().length < 512) {
                logger.debug("[{}] {} mapping [{}], source [{}]", index(), op, mappingType, incomingMappingSource.string());
            } else if (logger.isTraceEnabled()) {
                logger.trace("[{}] {} mapping [{}], source [{}]", index(), op, mappingType, incomingMappingSource.string());
            } else {
                logger.debug("[{}] {} mapping [{}] (source suppressed due to length, use TRACE level if needed)", index(), op, mappingType);
            }

            // refresh mapping can happen when the parsing/merging of the mapping from the metadata doesn't result in the same
            // mapping, in this case, we send to the cluster-manager to refresh its own version of the mappings (to conform with the
            // merge version of it, which it does when refreshing the mappings), and warn log it.
            if (documentMapper().mappingSource().equals(incomingMappingSource) == false) {
                logger.debug(
                    "[{}] parsed mapping [{}], and got different sources\noriginal:\n{}\nparsed:\n{}",
                    index(),
                    mappingType,
                    incomingMappingSource,
                    documentMapper().mappingSource()
                );

                requireRefresh = true;
            }
        }

        return requireRefresh;
    }

    private void assertMappingVersion(
        final IndexMetadata currentIndexMetadata,
        final IndexMetadata newIndexMetadata,
        final Map<String, DocumentMapper> updatedEntries
    ) throws IOException {
        if (Assertions.ENABLED && currentIndexMetadata != null) {
            if (currentIndexMetadata.getMappingVersion() == newIndexMetadata.getMappingVersion()) {
                // if the mapping version is unchanged, then there should not be any updates and all mappings should be the same
                assert updatedEntries.isEmpty() : updatedEntries;

                MappingMetadata mapping = newIndexMetadata.mapping();
                if (mapping != null) {
                    final CompressedXContent currentSource = currentIndexMetadata.mapping().source();
                    final CompressedXContent newSource = mapping.source();
                    assert currentSource.equals(newSource) : "expected current mapping ["
                        + currentSource
                        + "] for type ["
                        + mapping.type()
                        + "] "
                        + "to be the same as new mapping ["
                        + newSource
                        + "]";
                    final CompressedXContent mapperSource = new CompressedXContent(Strings.toString(MediaTypeRegistry.JSON, mapper));
                    assert currentSource.equals(mapperSource) : "expected current mapping ["
                        + currentSource
                        + "] for type ["
                        + mapping.type()
                        + "] "
                        + "to be the same as new mapping ["
                        + mapperSource
                        + "]";
                }

            } else {
                // the mapping version should increase, there should be updates, and the mapping should be different
                final long currentMappingVersion = currentIndexMetadata.getMappingVersion();
                final long newMappingVersion = newIndexMetadata.getMappingVersion();
                assert currentMappingVersion < newMappingVersion : "expected current mapping version ["
                    + currentMappingVersion
                    + "] "
                    + "to be less than new mapping version ["
                    + newMappingVersion
                    + "]";
                assert updatedEntries.isEmpty() == false;
                for (final DocumentMapper documentMapper : updatedEntries.values()) {
                    final MappingMetadata currentMapping = currentIndexMetadata.mapping();
                    assert currentMapping == null || documentMapper.type().equals(currentMapping.type());
                    if (currentMapping != null) {
                        final CompressedXContent currentSource = currentMapping.source();
                        final CompressedXContent newSource = documentMapper.mappingSource();
                        assert currentSource.equals(newSource) == false : "expected current mapping ["
                            + currentSource
                            + "] for type ["
                            + documentMapper.type()
                            + "] "
                            + "to be different than new mapping";
                    }
                }
            }
        }
    }

    public void merge(Map<String, Map<String, Object>> mappings, MergeReason reason) {
        Map<String, CompressedXContent> mappingSourcesCompressed = new LinkedHashMap<>(mappings.size());
        for (Map.Entry<String, Map<String, Object>> entry : mappings.entrySet()) {
            try {
                mappingSourcesCompressed.put(
                    entry.getKey(),
                    new CompressedXContent(XContentFactory.jsonBuilder().map(entry.getValue()).toString())
                );
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
            }
        }

        internalMerge(mappingSourcesCompressed, reason);
    }

    public void merge(String type, Map<String, Object> mappings, MergeReason reason) throws IOException {
        CompressedXContent content = new CompressedXContent(XContentFactory.jsonBuilder().map(mappings).toString());
        internalMerge(Collections.singletonMap(type, content), reason);
    }

    public void merge(IndexMetadata indexMetadata, MergeReason reason) {
        internalMerge(indexMetadata, reason);
    }

    public DocumentMapper merge(String type, CompressedXContent mappingSource, MergeReason reason) {
        return internalMerge(Collections.singletonMap(type, mappingSource), reason).values().iterator().next();
    }

    private synchronized Map<String, DocumentMapper> internalMerge(IndexMetadata indexMetadata, MergeReason reason) {
        assert reason != MergeReason.MAPPING_UPDATE_PREFLIGHT;
        Map<String, CompressedXContent> map = new LinkedHashMap<>();
        MappingMetadata mappingMetadata = indexMetadata.mapping();
        if (mappingMetadata != null) {
            map.put(mappingMetadata.type(), mappingMetadata.source());
        }
        return internalMerge(map, reason);
    }

    private synchronized Map<String, DocumentMapper> internalMerge(Map<String, CompressedXContent> mappings, MergeReason reason) {
        DocumentMapper documentMapper = null;
        for (Map.Entry<String, CompressedXContent> entry : mappings.entrySet()) {
            String type = entry.getKey();
            if (documentMapper != null) {
                throw new IllegalArgumentException("Cannot put multiple mappings: " + mappings.keySet());
            }

            try {
                documentMapper = documentParser.parse(type, entry.getValue());
            } catch (Exception e) {
                throw new MapperParsingException("Failed to parse mapping [{}]: {}", e, entry.getKey(), e.getMessage());
            }
        }

        return internalMerge(documentMapper, reason);
    }

    static void validateTypeName(String type) {
        if (type.length() == 0) {
            throw new InvalidTypeNameException("mapping type name is empty");
        }
        if (type.length() > 255) {
            throw new InvalidTypeNameException(
                "mapping type name [" + type + "] is too long; limit is length 255 but was [" + type.length() + "]"
            );
        }
        if (type.charAt(0) == '_' && SINGLE_MAPPING_NAME.equals(type) == false) {
            throw new InvalidTypeNameException(
                "mapping type name [" + type + "] can't start with '_' unless it is called [" + SINGLE_MAPPING_NAME + "]"
            );
        }
        if (type.contains("#")) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] should not include '#' in it");
        }
        if (type.contains(",")) {
            throw new InvalidTypeNameException("mapping type name [" + type + "] should not include ',' in it");
        }
        if (type.charAt(0) == '.') {
            throw new IllegalArgumentException("mapping type name [" + type + "] must not start with a '.'");
        }
    }

    private synchronized Map<String, DocumentMapper> internalMerge(DocumentMapper mapper, MergeReason reason) {
        Map<String, DocumentMapper> results = new LinkedHashMap<>(2);
        DocumentMapper newMapper = null;
        if (mapper != null) {
            // check naming
            validateTypeName(mapper.type());

            // compute the merged DocumentMapper
            DocumentMapper oldMapper = this.mapper;
            if (oldMapper != null) {
                newMapper = oldMapper.merge(mapper.mapping(), reason);
            } else {
                newMapper = mapper;
            }

            newMapper.root().fixRedundantIncludes();
            newMapper.validate(indexSettings, reason != MergeReason.MAPPING_RECOVERY);
            results.put(newMapper.type(), newMapper);
        }

        // make structures immutable
        results = Collections.unmodifiableMap(results);

        if (reason == MergeReason.MAPPING_UPDATE_PREFLIGHT) {
            return results;
        }

        // commit the change
        if (newMapper != null) {
            this.mapper = newMapper;
        }

        assert results.values().stream().allMatch(this::assertSerialization);

        // initialize composite fields post merge
        this.compositeMappedFieldTypes = getCompositeFieldTypesFromMapper();
        buildCompositeFieldLookup();
        return results;
    }

    private void buildCompositeFieldLookup() {
        Set<String> fieldsPartOfCompositeMappings = new HashSet<>();
        for (CompositeMappedFieldType fieldType : compositeMappedFieldTypes) {
            fieldsPartOfCompositeMappings.addAll(fieldType.fields());
        }
        this.fieldsPartOfCompositeMappings = fieldsPartOfCompositeMappings;
    }

    private boolean assertSerialization(DocumentMapper mapper) {
        // capture the source now, it may change due to concurrent parsing
        final CompressedXContent mappingSource = mapper.mappingSource();
        DocumentMapper newMapper = parse(mapper.type(), mappingSource);

        if (newMapper.mappingSource().equals(mappingSource) == false) {
            throw new IllegalStateException(
                "DocumentMapper serialization result is different from source. \n--> Source ["
                    + mappingSource
                    + "]\n--> Result ["
                    + newMapper.mappingSource()
                    + "]"
            );
        }
        return true;
    }

    public DocumentMapper parse(String mappingType, CompressedXContent mappingSource) throws MapperParsingException {
        return documentParser.parse(mappingType, mappingSource);
    }

    /**
     * Return the document mapper, or {@code null} if no mapping has been put yet.
     */
    public DocumentMapper documentMapper() {
        return mapper;
    }

    /**
     * Returns {@code true} if the given {@code mappingSource} includes a type
     * as a top-level object.
     */
    public static boolean isMappingSourceTyped(String type, Map<String, Object> mapping) {
        return mapping.size() == 1 && mapping.keySet().iterator().next().equals(type);
    }

    public static boolean isMappingSourceTyped(String type, CompressedXContent mappingSource) {
        Map<String, Object> root = XContentHelper.convertToMap(mappingSource.compressedReference(), true, MediaTypeRegistry.JSON).v2();
        return isMappingSourceTyped(type, root);
    }

    /**
     * Resolves a type from a mapping-related request into the type that should be used when
     * merging and updating mappings.
     * <p>
     * If the special `_doc` type is provided, then we replace it with the actual type that is
     * being used in the mappings. This allows typeless APIs such as 'index' or 'put mappings'
     * to work against indices with a custom type name.
     */
    public String resolveDocumentType(String type) {
        if (MapperService.SINGLE_MAPPING_NAME.equals(type)) {
            if (mapper != null) {
                return mapper.type();
            }
        }
        return type;
    }

    /**
     * Returns the document mapper created, including a mapping update if the
     * type has been dynamically created.
     */
    public DocumentMapperForType documentMapperWithAutoCreate() {
        DocumentMapper mapper = documentMapper();
        if (mapper != null) {
            return new DocumentMapperForType(mapper, null);
        }
        mapper = parse(SINGLE_MAPPING_NAME, null);
        return new DocumentMapperForType(mapper, mapper.mapping());
    }

    /**
     * Given the full name of a field, returns its {@link MappedFieldType}.
     */
    public MappedFieldType fieldType(String fullName) {
        return this.mapper == null ? null : this.mapper.fieldTypes().get(fullName);
    }

    /**
     * Returns all the fields that match the given pattern. If the pattern is prefixed with a type
     * then the fields will be returned with a type prefix.
     */
    public Set<String> simpleMatchToFullName(String pattern) {
        if (Regex.isSimpleMatchPattern(pattern) == false) {
            // no wildcards
            return Collections.singleton(pattern);
        }
        return this.mapper == null ? Collections.emptySet() : this.mapper.fieldTypes().simpleMatchToFullName(pattern);
    }

    /**
     * Given a field name, returns its possible paths in the _source. For example,
     * the 'source path' for a multi-field is the path to its parent field.
     */
    public Set<String> sourcePath(String fullName) {
        return this.mapper == null ? Collections.emptySet() : this.mapper.fieldTypes().sourcePaths(fullName);
    }

    /**
     * Returns all mapped field types.
     */
    public Iterable<MappedFieldType> fieldTypes() {
        return this.mapper == null ? Collections.emptySet() : this.mapper.fieldTypes();
    }

    public boolean isCompositeIndexPresent() {
        return this.mapper != null && !getCompositeFieldTypes().isEmpty();
    }

    public Set<CompositeMappedFieldType> getCompositeFieldTypes() {
        return compositeMappedFieldTypes;
    }

    private Set<CompositeMappedFieldType> getCompositeFieldTypesFromMapper() {
        Set<CompositeMappedFieldType> compositeMappedFieldTypes = new HashSet<>();
        if (this.mapper == null) {
            return Collections.emptySet();
        }
        for (MappedFieldType type : this.mapper.fieldTypes()) {
            if (type instanceof CompositeMappedFieldType) {
                compositeMappedFieldTypes.add((CompositeMappedFieldType) type);
            }
        }
        return compositeMappedFieldTypes;
    }

    public boolean isFieldPartOfCompositeIndex(String field) {
        return fieldsPartOfCompositeMappings.contains(field);
    }

    public ObjectMapper getObjectMapper(String name) {
        return this.mapper == null ? null : this.mapper.objectMappers().get(name);
    }

    /**
     * Given a type (eg. long, string, ...), return an anonymous field mapper that can be used for search operations.
     */
    public MappedFieldType unmappedFieldType(String type) {
        if (type.equals("string")) {
            deprecationLogger.deprecate("unmapped_type_string", "[unmapped_type:string] should be replaced with [unmapped_type:keyword]");
            type = "keyword";
        }
        MappedFieldType fieldType = unmappedFieldTypes.get(type);
        if (fieldType == null) {
            final Mapper.TypeParser.ParserContext parserContext = documentMapperParser().parserContext();
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            if (typeParser == null) {
                throw new IllegalArgumentException("No mapper found for type [" + type + "]");
            }
            final Mapper.Builder<?> builder = typeParser.parse("__anonymous_" + type, emptyMap(), parserContext);
            final BuilderContext builderContext = new BuilderContext(indexSettings.getSettings(), new ContentPath(1));
            fieldType = ((FieldMapper) builder.build(builderContext)).fieldType();

            // There is no need to synchronize writes here. In the case of concurrent access, we could just
            // compute some mappers several times, which is not a big deal
            Map<String, MappedFieldType> newUnmappedFieldTypes = new HashMap<>(unmappedFieldTypes);
            newUnmappedFieldTypes.put(type, fieldType);
            unmappedFieldTypes = unmodifiableMap(newUnmappedFieldTypes);
        }
        return fieldType;
    }

    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    public Analyzer searchQuoteAnalyzer() {
        return this.searchQuoteAnalyzer;
    }

    /**
     * Returns <code>true</code> if fielddata is enabled for the {@link IdFieldMapper} field, <code>false</code> otherwise.
     */
    public boolean isIdFieldDataEnabled() {
        return idFieldDataEnabled.getAsBoolean();
    }

    @Override
    public void close() throws IOException {
        indexAnalyzers.close();
    }

    /**
     * @return Whether a field is a metadata field.
     * this method considers all mapper plugins
     */
    public boolean isMetadataField(String field) {
        return mapperRegistry.isMetadataField(field);
    }

    /**
     * Returns a set containing the registered metadata fields
     */
    public Set<String> getMetadataFields() {
        return Collections.unmodifiableSet(mapperRegistry.getMetadataMapperParsers().keySet());
    }

    /**
     * An analyzer wrapper that can lookup fields within the index mappings
     */
    final class MapperAnalyzerWrapper extends DelegatingAnalyzerWrapper {

        private final Analyzer defaultAnalyzer;
        private final Function<MappedFieldType, Analyzer> extractAnalyzer;

        MapperAnalyzerWrapper(Analyzer defaultAnalyzer, Function<MappedFieldType, Analyzer> extractAnalyzer) {
            super(Analyzer.PER_FIELD_REUSE_STRATEGY);
            this.defaultAnalyzer = defaultAnalyzer;
            this.extractAnalyzer = extractAnalyzer;
        }

        @Override
        protected Analyzer getWrappedAnalyzer(String fieldName) {
            MappedFieldType fieldType = fieldType(fieldName);
            if (fieldType != null) {
                Analyzer analyzer = extractAnalyzer.apply(fieldType);
                if (analyzer != null) {
                    return analyzer;
                }
            }
            return defaultAnalyzer;
        }
    }

    public synchronized List<String> reloadSearchAnalyzers(AnalysisRegistry registry) throws IOException {
        logger.info("reloading search analyzers");
        // refresh indexAnalyzers and search analyzers
        final Map<String, TokenizerFactory> tokenizerFactories = registry.buildTokenizerFactories(indexSettings);
        final Map<String, CharFilterFactory> charFilterFactories = registry.buildCharFilterFactories(indexSettings);
        final Map<String, TokenFilterFactory> tokenFilterFactories = registry.buildTokenFilterFactories(indexSettings);
        final Map<String, Settings> settings = indexSettings.getSettings().getGroups("index.analysis.analyzer");
        final List<String> reloadedAnalyzers = new ArrayList<>();
        for (NamedAnalyzer namedAnalyzer : indexAnalyzers.getAnalyzers().values()) {
            if (namedAnalyzer.analyzer() instanceof ReloadableCustomAnalyzer) {
                ReloadableCustomAnalyzer analyzer = (ReloadableCustomAnalyzer) namedAnalyzer.analyzer();
                String analyzerName = namedAnalyzer.name();
                Settings analyzerSettings = settings.get(analyzerName);
                analyzer.reload(analyzerName, analyzerSettings, tokenizerFactories, charFilterFactories, tokenFilterFactories);
                reloadedAnalyzers.add(analyzerName);
            }
        }
        return reloadedAnalyzers;
    }

}
