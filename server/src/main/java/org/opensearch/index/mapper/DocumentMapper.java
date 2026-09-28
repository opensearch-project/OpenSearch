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

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchGenerationException;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IndexSortConfig;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities.FieldScope;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.index.mapper.MetadataFieldMapper.TypeParser;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

/**
 * The OpenSearch DocumentMapper
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DocumentMapper implements ToXContentFragment {

    /**
     * Builder for the Document Field Mapper
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Builder {

        private final Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = new LinkedHashMap<>();

        private final RootObjectMapper rootObjectMapper;

        private Map<String, Object> meta;

        private final Mapper.BuilderContext builderContext;

        private final long newVersion;

        public Builder(RootObjectMapper.Builder builder, MapperService mapperService) {
            this(builder, mapperService, null);
        }

        public Builder(RootObjectMapper.Builder builder, MapperService mapperService, @Nullable DataFormatRegistry dataFormatRegistry) {
            final IndexSettings is = mapperService.getIndexSettings();
            final Settings indexSettings = is.getSettings();
            final Consumer<MappedFieldType> assigner = dataFormatRegistry != null
                ? fieldType -> dataFormatRegistry.assignCapabilities(fieldType, is)
                : null;
            this.builderContext = new Mapper.BuilderContext(indexSettings, new ContentPath(1), assigner);
            this.rootObjectMapper = builder.build(builderContext);

            final DocumentMapper existingMapper = mapperService.documentMapper();
            this.newVersion = existingMapper == null ? 1L : existingMapper.getVersion() + 1L;
            final Map<String, TypeParser> metadataMapperParsers = mapperService.mapperRegistry.getMetadataMapperParsers();
            for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : metadataMapperParsers.entrySet()) {
                final String name = entry.getKey();
                final MetadataFieldMapper existingMetadataMapper = existingMapper == null
                    ? null
                    : (MetadataFieldMapper) existingMapper.mappers().getMapper(name);
                final MetadataFieldMapper metadataMapper;
                if (existingMetadataMapper == null) {
                    final TypeParser parser = entry.getValue();
                    metadataMapper = parser.getDefault(mapperService.fieldType(name), mapperService.documentMapperParser().parserContext());
                } else {
                    metadataMapper = existingMetadataMapper;
                }
                metadataMappers.put(metadataMapper.getClass(), metadataMapper);
            }
        }

        /**
         * Recursively walks the mapper tree and assigns capability maps to all non-metadata field types.
         */
        private static void assignCapabilitiesRecursive(Mapper mapper, Mapper.BuilderContext context) {
            if (mapper instanceof FieldMapper) {
                context.assignCapabilities(((FieldMapper) mapper).fieldType());
            }
            for (Mapper child : mapper) {
                assignCapabilitiesRecursive(child, context);
            }
        }

        public Builder meta(Map<String, Object> meta) {
            this.meta = meta;
            return this;
        }

        public Builder put(MetadataFieldMapper.Builder mapper) {
            MetadataFieldMapper metadataMapper = mapper.build(builderContext);
            metadataMappers.put(metadataMapper.getClass(), metadataMapper);
            return this;
        }

        public DocumentMapper build(MapperService mapperService) {
            Objects.requireNonNull(rootObjectMapper, "Mapper builder must have the root object mapper set");
            Mapping mapping = new Mapping(
                mapperService.getIndexSettings().getIndexVersionCreated(),
                rootObjectMapper,
                metadataMappers.values().toArray(new MetadataFieldMapper[0]),
                meta
            );
            return new DocumentMapper(mapperService, mapping, newVersion);
        }
    }

    private final MapperService mapperService;

    private final String type;
    private final Text typeText;

    private final CompressedXContent mappingSource;

    private final Mapping mapping;

    private final DocumentParser documentParser;

    private final MappingLookup fieldMappers;

    private final MetadataFieldMapper[] deleteTombstoneMetadataFieldMappers;
    private final MetadataFieldMapper[] noopTombstoneMetadataFieldMappers;

    private final long version;

    private final Set<String> indexMarkers;

    public DocumentMapper(MapperService mapperService, Mapping mapping) {
        this(mapperService, mapping, 1L);
    }

    public DocumentMapper(MapperService mapperService, Mapping mapping, long version) {
        this.mapperService = mapperService;
        this.type = mapping.root().name();
        this.typeText = new Text(this.type);
        final IndexSettings indexSettings = mapperService.getIndexSettings();
        // Must precede serialization: the source below is what gets stored and returned by GET _mapping.
        final DataFormatRegistry registry = mapperService.documentMapperParser().getDataFormatRegistry();
        final Set<String> markers = new HashSet<>();
        if (indexSettings.isPluggableDataFormatEnabled() && registry != null) {
            mapping = alignWithDataFormats(mapping, registry, indexSettings, markers);
        }
        this.indexMarkers = Set.copyOf(markers);
        this.mapping = mapping;
        this.documentParser = new DocumentParser(indexSettings, mapperService.documentMapperParser(), this);

        final IndexAnalyzers indexAnalyzers = mapperService.getIndexAnalyzers();
        this.fieldMappers = MappingLookup.fromMapping(
            this.mapping,
            indexAnalyzers.getDefaultIndexAnalyzer(),
            mapperService.documentMapperParser()
        );

        try {
            mappingSource = new CompressedXContent(this, ToXContent.EMPTY_PARAMS);
        } catch (Exception e) {
            throw new OpenSearchGenerationException("failed to serialize source for type [" + type + "]", e);
        }

        final Collection<String> deleteTombstoneMetadataFields = Arrays.asList(
            VersionFieldMapper.NAME,
            IdFieldMapper.NAME,
            RoutingFieldMapper.NAME,
            SeqNoFieldMapper.NAME,
            SeqNoFieldMapper.PRIMARY_TERM_NAME,
            SeqNoFieldMapper.TOMBSTONE_NAME
        );
        this.deleteTombstoneMetadataFieldMappers = Stream.of(mapping.metadataMappers)
            .filter(field -> deleteTombstoneMetadataFields.contains(field.name()))
            .toArray(MetadataFieldMapper[]::new);
        final Collection<String> noopTombstoneMetadataFields = Arrays.asList(
            VersionFieldMapper.NAME,
            SeqNoFieldMapper.NAME,
            SeqNoFieldMapper.PRIMARY_TERM_NAME,
            SeqNoFieldMapper.TOMBSTONE_NAME
        );
        this.noopTombstoneMetadataFieldMappers = Stream.of(mapping.metadataMappers)
            .filter(field -> noopTombstoneMetadataFields.contains(field.name()))
            .toArray(MetadataFieldMapper[]::new);

        this.version = version;
    }

    /**
     * Assigns data format capabilities to every field type, then records in the mapping what the format cannot
     * search, so the stored source describes what the index holds. Only two shapes can be recorded:
     * <ul>
     *   <li>a nested object whose scope the format cannot search gets {@code index: false} on the object
     *       ({@link ObjectMapper.Nested#isIndexed()}); nothing under it may set {@code index} or be searchable;</li>
     *   <li>a {@code flat_object} outside nested that the format cannot search gets {@code index: false}.</li>
     * </ul>
     * The format being unable to search any other field fails the mapping. An {@code index: false} spelled out on
     * either shape is added to {@code markers}; {@link MapperService} accepts those only when re-read from the
     * index's own mapping. Idempotent; storage capabilities are never rewritten.
     */
    private Mapping alignWithDataFormats(Mapping mapping, DataFormatRegistry registry, IndexSettings indexSettings, Set<String> markers) {
        try {
            boolean consulted = assignCapabilitiesRecursive(mapping.root(), registry, indexSettings, FieldScope.ROOT, false);
            // Metadata fields always exist, so a mapping with no regular leaf (e.g. only empty nested objects) is still aligned.
            for (MetadataFieldMapper metadataMapper : mapping.metadataMappers) {
                consulted |= registry.assignCapabilities(metadataMapper.fieldType(), indexSettings, FieldScope.ROOT);
            }
            if (consulted == false) {
                return mapping;
            }
            final Mapper alignedRoot = new SearchRecorder(registry, indexSettings, markers).record(mapping.root());
            if (alignedRoot == mapping.root()) {
                return mapping;
            }
            final Mapping aligned = mapping.mappingUpdate(alignedRoot);
            assignCapabilitiesRecursive(aligned.root(), registry, indexSettings, FieldScope.ROOT, false);
            return aligned;
        } catch (UnsupportedOperationException e) {
            // 400 rather than an unserializable 500 on the put-mapping path.
            throw new MapperParsingException(e.getMessage(), e);
        }
    }

    /** Paths of nested objects and flat_object fields whose {@code index: false} was spelled out in the parsed mapping. */
    Set<String> indexMarkers() {
        return indexMarkers;
    }

    /**
     * Rejects spelled-out {@code index: false} markers unless {@code existing} already stores the same one: that is
     * how a dynamic mapping update or a re-PUT of the index's own mapping looks. {@code null} rejects them all.
     */
    void rejectUnrecordedIndexMarkers(@Nullable DocumentMapper existing) {
        for (String path : indexMarkers) {
            if (existing == null || existing.storesIndexMarker(path) == false) {
                throw new MapperParsingException("[index] on [" + path + "] is set by the server and must not be specified in the mapping");
            }
        }
    }

    private boolean storesIndexMarker(String path) {
        final ObjectMapper objectMapper = objectMappers().get(path);
        if (objectMapper != null) {
            return objectMapper.nested().isNested() && objectMapper.nested().isIndexed() == false;
        }
        return mappers().getMapper(path) instanceof FlatObjectFieldMapper flatObject && flatObject.fieldType().isSearchable() == false;
    }

    /** Bottom-up rewrite per {@link #alignWithDataFormats}; returns a mapper itself when nothing under it changed. */
    private static final class SearchRecorder {
        private final DataFormatRegistry registry;
        private final IndexSettings indexSettings;
        private final Set<String> markers;

        SearchRecorder(DataFormatRegistry registry, IndexSettings indexSettings, Set<String> markers) {
            this.registry = registry;
            this.indexSettings = indexSettings;
            this.markers = markers;
        }

        Mapper record(Mapper mapper) {
            if (mapper instanceof ObjectMapper objectMapper) {
                return objectMapper.nested().isNested() ? recordNested(objectMapper) : withChildren(objectMapper, this::record);
            }
            if (mapper instanceof FieldMapper fieldMapper) {
                return recordField(fieldMapper, withSubFields(fieldMapper, this::record));
            }
            return mapper;
        }

        /** Whole subtree at once; inner nested objects inherit the outer flag. */
        private Mapper recordNested(ObjectMapper nested) {
            final String path = nested.fullPath();
            final boolean searchable = canSearch(new KeywordFieldMapper.KeywordFieldType(path, true, false, Map.of()), FieldScope.NESTED);
            if (nested.nested().isIndexExplicit()) {
                if (searchable || nested.nested().isIndexed()) {
                    throw new MapperParsingException("nested object [" + path + "] must not set [index]: it is set by the server");
                }
                markers.add(path);
            }
            checkNestedScope(nested, path, searchable);
            return searchable || nested.nested().isIndexed() == false ? nested : nested.withNestedIndexDisabled();
        }

        private static void checkNestedScope(Mapper parent, String nestedPath, boolean searchable) {
            for (Mapper child : parent) {
                if (child instanceof ObjectMapper inner && inner.nested().isNested() && inner.nested().isIndexExplicit()) {
                    throw new MapperParsingException(
                        "nested object [" + inner.fullPath() + "] must not set [index]: it is set by the server"
                    );
                }
                if (child instanceof FieldMapper leaf) {
                    if (searchable == false && leaf.isIndexExplicit()) {
                        throw new MapperParsingException(
                            "field ["
                                + leaf.name()
                                + "] must not set [index]: the data format cannot search under nested object ["
                                + nestedPath
                                + "], which is recorded as [index: false] on the object"
                        );
                    }
                    // A searchable leaf must get the same answer from the format as its nested scope.
                    if (leaf.fieldType().isSearchable() && searchDeclined(leaf.fieldType()) == searchable) {
                        throw new MapperParsingException(
                            "field ["
                                + leaf.name()
                                + "]: the data format supports search for some fields under nested object ["
                                + nestedPath
                                + "] but not others, which is not supported"
                        );
                    }
                }
                checkNestedScope(child, nestedPath, searchable);
            }
        }

        private Mapper recordField(FieldMapper original, FieldMapper aligned) {
            final MappedFieldType fieldType = original.fieldType();
            final boolean flatObject = FlatObjectFieldMapper.CONTENT_TYPE.equals(fieldType.typeName());
            if (fieldType.isSearchable()) {
                if (searchDeclined(fieldType) == false) {
                    return aligned;
                }
                if (flatObject == false) {
                    throw new MapperParsingException(
                        "field ["
                            + original.name()
                            + "] of type ["
                            + fieldType.typeName()
                            + "]: the data format cannot search it, which is only supported for nested objects and flat_object fields"
                    );
                }
                if (original.isIndexExplicit()) {
                    throw new MapperParsingException(
                        "field [" + original.name() + "] must not set [index: true]: the data format cannot search flat_object fields"
                    );
                }
                return aligned.withIndexDisabled(parentContext(original, indexSettings.getSettings()));
            }
            if (flatObject
                && original.isIndexExplicit()
                && canSearch(new FlatObjectFieldMapper.FlatObjectFieldType(original.name(), null, true, false), FieldScope.ROOT) == false) {
                markers.add(original.name());
            }
            return aligned;
        }

        /** Asks the format whether it would search {@code probe} in {@code scope}. */
        private boolean canSearch(MappedFieldType probe, FieldScope scope) {
            registry.assignCapabilities(probe, indexSettings, scope);
            return searchDeclined(probe) == false;
        }
    }

    /** Applies {@code rewrite} to each child; returns {@code objectMapper} itself if no child changed. */
    private static ObjectMapper withChildren(ObjectMapper objectMapper, UnaryOperator<Mapper> rewrite) {
        final Map<String, Mapper> replacements = new LinkedHashMap<>();
        for (Mapper child : objectMapper) {
            Mapper rewritten = rewrite.apply(child);
            if (rewritten != child) {
                replacements.put(child.simpleName(), rewritten);
            }
        }
        return replacements.isEmpty() ? objectMapper : objectMapper.withReplacedChildren(replacements);
    }

    /** Applies {@code rewrite} to each multi-field; returns {@code fieldMapper} itself if none changed. */
    private static FieldMapper withSubFields(FieldMapper fieldMapper, UnaryOperator<Mapper> rewrite) {
        final Map<String, FieldMapper> replacements = new LinkedHashMap<>();
        for (Mapper subField : fieldMapper.multiFields) {
            Mapper rewritten = rewrite.apply(subField);
            if (rewritten != subField) {
                replacements.put(subField.simpleName(), (FieldMapper) rewritten);
            }
        }
        return replacements.isEmpty() ? fieldMapper : fieldMapper.withMultiFields(fieldMapper.multiFields.withReplaced(replacements));
    }

    /** Builder context rooted at the field's parent path, so a rebuilt copy keeps its full name. */
    private static Mapper.BuilderContext parentContext(FieldMapper fieldMapper, Settings indexSettings) {
        return new Mapper.BuilderContext(indexSettings, ParametrizedFieldMapper.parentPath(fieldMapper.name(), fieldMapper.simpleName()));
    }

    /** True when the field type asks for its search capability but no data format was assigned it. */
    private static boolean searchDeclined(MappedFieldType fieldType) {
        if (fieldType.isSearchable() == false) {
            return false;
        }
        final FieldTypeCapabilities.Capability search = fieldType.searchCapability();
        for (Set<FieldTypeCapabilities.Capability> assigned : fieldType.getCapabilityMap().values()) {
            if (assigned.contains(search)) {
                return false;
            }
        }
        return true;
    }

    public Mapping mapping() {
        return mapping;
    }

    public String type() {
        return this.type;
    }

    public Text typeText() {
        return this.typeText;
    }

    public Map<String, Object> meta() {
        return mapping.meta;
    }

    public CompressedXContent mappingSource() {
        return this.mappingSource;
    }

    public long getVersion() {
        return this.version;
    }

    public RootObjectMapper root() {
        return mapping.root;
    }

    public <T extends MetadataFieldMapper> T metadataMapper(Class<T> type) {
        return mapping.metadataMapper(type);
    }

    public SourceFieldMapper sourceMapper() {
        return metadataMapper(SourceFieldMapper.class);
    }

    public IdFieldMapper idFieldMapper() {
        return metadataMapper(IdFieldMapper.class);
    }

    public RoutingFieldMapper routingFieldMapper() {
        return metadataMapper(RoutingFieldMapper.class);
    }

    public IndexFieldMapper IndexFieldMapper() {
        return metadataMapper(IndexFieldMapper.class);
    }

    public boolean hasNestedObjects() {
        return mappers().hasNested();
    }

    public MappingLookup mappers() {
        return this.fieldMappers;
    }

    FieldTypeLookup fieldTypes() {
        return mappers().fieldTypes();
    }

    public Map<String, ObjectMapper> objectMappers() {
        return mappers().objectMappers();
    }

    public ParsedDocument parse(SourceToParse source) throws MapperParsingException {
        return documentParser.parseDocument(source, mapping.metadataMappers);
    }

    public ParsedDocument parse(SourceToParse source, DocumentInput documentInput) throws MapperParsingException {
        return documentParser.parseDocument(source, mapping.metadataMappers, documentInput);
    }

    public ParsedDocument createDeleteTombstoneDoc(String index, String id) throws MapperParsingException {
        return createDeleteTombstoneDoc(index, id, null);
    }

    public ParsedDocument createDeleteTombstoneDoc(String index, String id, @Nullable String routing) throws MapperParsingException {
        final SourceToParse emptySource = new SourceToParse(index, id, new BytesArray("{}"), MediaTypeRegistry.JSON, routing);
        return documentParser.parseDocument(emptySource, deleteTombstoneMetadataFieldMappers).toTombstone();
    }

    public ParsedDocument createNoopTombstoneDoc(String index, String reason) throws MapperParsingException {
        final String id = ""; // _id won't be used.
        final SourceToParse sourceToParse = new SourceToParse(index, id, new BytesArray("{}"), MediaTypeRegistry.JSON);
        final ParsedDocument parsedDoc = documentParser.parseDocument(sourceToParse, noopTombstoneMetadataFieldMappers).toTombstone();
        // Store the reason of a noop as a raw string in the _source field
        final BytesRef byteRef = new BytesRef(reason);
        parsedDoc.rootDoc().add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
        return parsedDoc;
    }

    /**
     * Returns the best nested {@link ObjectMapper} instances that is in the scope of the specified nested docId.
     */
    public ObjectMapper findNestedObjectMapper(int nestedDocId, SearchContext sc, LeafReaderContext context) throws IOException {
        if (sc instanceof NestedQueryBuilder.NestedInnerHitSubContext) {
            ObjectMapper objectMapper = ((NestedQueryBuilder.NestedInnerHitSubContext) sc).getChildObjectMapper();
            assert objectMappers().containsKey(objectMapper.fullPath());
            assert containSubDocIdWithObjectMapper(nestedDocId, objectMapper, sc, context);
            return objectMapper;
        }
        ObjectMapper nestedObjectMapper = null;
        for (ObjectMapper objectMapper : objectMappers().values()) {
            if (containSubDocIdWithObjectMapper(nestedDocId, objectMapper, sc, context)) {
                if (nestedObjectMapper == null) {
                    nestedObjectMapper = objectMapper;
                } else {
                    if (nestedObjectMapper.fullPath().length() < objectMapper.fullPath().length()) {
                        nestedObjectMapper = objectMapper;
                    }
                }
            }
        }
        return nestedObjectMapper;
    }

    private boolean containSubDocIdWithObjectMapper(int nestedDocId, ObjectMapper objectMapper, SearchContext sc, LeafReaderContext context)
        throws IOException {
        if (!objectMapper.nested().isNested()) {
            return false;
        }
        Query filter = objectMapper.nestedTypeFilter();
        if (filter == null) {
            return false;
        }
        // We can pass down 'null' as acceptedDocs, because nestedDocId is a doc to be fetched and
        // therefore is guaranteed to be a live doc.
        BitSet nestedDocIds = sc.bitsetFilterCache().getBitSetProducer(filter).getBitSet(context);
        if (nestedDocIds != null && nestedDocIds.get(nestedDocId)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Recursively walks the mapper tree and assigns capability maps to all field types.
     *
     * @param fieldScope   the mapper's current field scope; descendants of a nested
     *                     {@link ObjectMapper} remain in {@link FieldScope#NESTED}
     * @param isMultiField whether {@code mapper} is a multi-field of its parent
     * @return {@code true} if a data format plugin was consulted for at least one field type
     */
    private static boolean assignCapabilitiesRecursive(
        Mapper mapper,
        DataFormatRegistry registry,
        IndexSettings indexSettings,
        FieldScope fieldScope,
        boolean isMultiField
    ) {
        boolean consulted = false;
        if (mapper instanceof FieldMapper) {
            consulted = registry.assignCapabilities(((FieldMapper) mapper).fieldType(), indexSettings, fieldScope);
            // For derived source: keyword fields with ignore_above/normalizer use a separate
            // rawValueFieldType to store the raw value for source reconstruction.
            if (mapper instanceof KeywordFieldMapper keywordFieldMapper) {
                KeywordFieldMapper.KeywordFieldType rawValueFieldType = keywordFieldMapper.getRawValueFieldType();
                if (rawValueFieldType != null && isMultiField == false) {
                    registry.assignCapabilities(rawValueFieldType, indexSettings, fieldScope);
                }
            }
        }
        FieldScope childScope = fieldScope == FieldScope.NESTED
            || (mapper instanceof ObjectMapper objectMapper && objectMapper.nested().isNested()) ? FieldScope.NESTED : FieldScope.ROOT;
        // A FieldMapper's children are its multi-fields.
        boolean childIsMultiField = mapper instanceof FieldMapper;
        for (Mapper child : mapper) {
            consulted |= assignCapabilitiesRecursive(child, registry, indexSettings, childScope, childIsMultiField);
        }
        return consulted;
    }

    public DocumentMapper merge(Mapping mapping, MergeReason reason) {
        Mapping merged = this.mapping.merge(mapping, reason);
        return new DocumentMapper(mapperService, merged, this.version + 1L);
    }

    public void validate(IndexSettings settings, boolean checkLimits) {
        this.mapping.validate(this.fieldMappers);
        if (settings.getIndexMetadata().isRoutingPartitionedIndex()) {
            if (routingFieldMapper().required() == false) {
                throw new IllegalArgumentException(
                    "mapping type ["
                        + type()
                        + "] must have routing "
                        + "required for partitioned index ["
                        + settings.getIndex().getName()
                        + "]"
                );
            }
        }

        // Indexing Sort with Nested Fields is only supported on & after Version 3.2.0
        if (settings.getIndexSortConfig().hasIndexSort() && hasNestedObjects()) {
            if (settings.getIndexVersionCreated().before(Version.V_3_2_0)) {
                throw new IllegalArgumentException("cannot have nested fields when index sort is activated");
            }

            /*
             * Index sorting works for regular fields across documents that may contain nested objects,
             * but sorting on fields inside nested objects is not supported. This validation checks
             * the index sort configuration and throws an exception if any sort field is inside
             * a nested object.
             */
            List<String> sortFields = settings.getValue(IndexSortConfig.INDEX_SORT_FIELD_SETTING);
            for (String sortField : sortFields) {
                Mapper mapper = this.fieldMappers.getMapper(sortField);
                if (mapper != null && mapper.name().contains(".")) {
                    String parentPath = mapper.name().substring(0, mapper.name().lastIndexOf('.'));
                    ObjectMapper nestedParent = objectMappers().get(parentPath);
                    if (nestedParent != null && nestedParent.nested().isNested()) {
                        throw new IllegalArgumentException(
                            "index sorting on nested fields is not supported: "
                                + "found nested sort field ["
                                + sortField
                                + "] in ["
                                + settings.getIndex().getName()
                                + "]"
                        );
                    }
                }
            }
        }

        if (checkLimits) {
            this.fieldMappers.checkLimits(settings);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return mapping.toXContent(builder, params);
    }

    @Override
    public String toString() {
        return "DocumentMapper{"
            + "mapperService="
            + mapperService
            + ", type='"
            + type
            + '\''
            + ", typeText="
            + typeText
            + ", mappingSource="
            + mappingSource
            + ", mapping="
            + mapping
            + ", documentParser="
            + documentParser
            + ", fieldMappers="
            + fieldMappers
            + ", objectMappers="
            + objectMappers()
            + ", hasNestedObjects="
            + hasNestedObjects()
            + ", deleteTombstoneMetadataFieldMappers="
            + Arrays.toString(deleteTombstoneMetadataFieldMappers)
            + ", noopTombstoneMetadataFieldMappers="
            + Arrays.toString(noopTombstoneMetadataFieldMappers)
            + '}';
    }
}
