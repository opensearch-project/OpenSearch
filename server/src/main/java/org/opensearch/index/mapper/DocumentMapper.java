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
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.mapper.MapperService.MergeReason;
import org.opensearch.index.mapper.MetadataFieldMapper.TypeParser;
import org.opensearch.index.query.NestedQueryBuilder;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
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

        public Builder(RootObjectMapper.Builder builder, MapperService mapperService) {
            final Settings indexSettings = mapperService.getIndexSettings().getSettings();
            this.builderContext = new Mapper.BuilderContext(indexSettings, new ContentPath(1));
            this.rootObjectMapper = builder.build(builderContext);

            final DocumentMapper existingMapper = mapperService.documentMapper();
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
            return new DocumentMapper(mapperService, mapping);
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

    public DocumentMapper(MapperService mapperService, Mapping mapping) {
        this.mapperService = mapperService;
        this.type = mapping.root().name();
        this.typeText = new Text(this.type);
        final IndexSettings indexSettings = mapperService.getIndexSettings();
        this.mapping = mapping;
        this.documentParser = new DocumentParser(indexSettings, mapperService.documentMapperParser(), this);

        final IndexAnalyzers indexAnalyzers = mapperService.getIndexAnalyzers();
        this.fieldMappers = MappingLookup.fromMapping(this.mapping, indexAnalyzers.getDefaultIndexAnalyzer());

        try {
            mappingSource = new CompressedXContent(this, ToXContent.EMPTY_PARAMS);
        } catch (Exception e) {
            throw new OpenSearchGenerationException("failed to serialize source for type [" + type + "]", e);
        }

        final Collection<String> deleteTombstoneMetadataFields = Arrays.asList(
            VersionFieldMapper.NAME,
            IdFieldMapper.NAME,
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

    public ParsedDocument createDeleteTombstoneDoc(String index, String id) throws MapperParsingException {
        final SourceToParse emptySource = new SourceToParse(index, id, new BytesArray("{}"), MediaTypeRegistry.JSON);
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

    public DocumentMapper merge(Mapping mapping, MergeReason reason) {
        Mapping merged = this.mapping.merge(mapping, reason);
        return new DocumentMapper(mapperService, merged);
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
        if (settings.getIndexSortConfig().hasIndexSort() && hasNestedObjects()) {
            throw new IllegalArgumentException("cannot have nested fields when index sort is activated");
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
