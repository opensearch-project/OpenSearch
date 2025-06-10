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

package org.opensearch.percolator;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.DelegatingAnalyzerWrapper;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.opensearch.OpenSearchException;
import org.opensearch.ResourceNotFoundException;
import org.opensearch.Version;
import org.opensearch.action.get.GetRequest;
import org.opensearch.common.SetOnce;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.analysis.FieldNameAnalyzer;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.Rewriteable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.opensearch.search.SearchService.ALLOW_EXPENSIVE_QUERIES;

public class PercolateQueryBuilder extends AbstractQueryBuilder<PercolateQueryBuilder> {
    public static final String NAME = "percolate";

    static final ParseField DOCUMENT_FIELD = new ParseField("document");
    static final ParseField DOCUMENTS_FIELD = new ParseField("documents");
    private static final ParseField NAME_FIELD = new ParseField("name");
    private static final ParseField QUERY_FIELD = new ParseField("field");
    private static final ParseField INDEXED_DOCUMENT_FIELD_INDEX = new ParseField("index");
    private static final ParseField INDEXED_DOCUMENT_FIELD_ID = new ParseField("id");
    private static final ParseField INDEXED_DOCUMENT_FIELD_ROUTING = new ParseField("routing");
    private static final ParseField INDEXED_DOCUMENT_FIELD_PREFERENCE = new ParseField("preference");
    private static final ParseField INDEXED_DOCUMENT_FIELD_VERSION = new ParseField("version");

    private final String field;
    private String name;
    private final List<BytesReference> documents;
    private final MediaType documentXContentType;

    private final String indexedDocumentIndex;
    private final String indexedDocumentId;
    private final String indexedDocumentRouting;
    private final String indexedDocumentPreference;
    private final Long indexedDocumentVersion;
    private final Supplier<BytesReference> documentSupplier;

    /**
     * Creates a percolator query builder instance for percolating a provided document.
     *
     * @param field                     The field that contains the percolator query
     * @param document                  The binary blob containing document to percolate
     * @param documentMediaType      The content type of the binary blob containing the document to percolate
     */
    public PercolateQueryBuilder(String field, BytesReference document, MediaType documentMediaType) {
        this(field, Collections.singletonList(document), documentMediaType);
    }

    /**
     * Creates a percolator query builder instance for percolating a provided document.
     *
     * @param field                     The field that contains the percolator query
     * @param documents                  The binary blob containing document to percolate
     * @param documentXContentType      The content type of the binary blob containing the document to percolate
     */
    public PercolateQueryBuilder(String field, List<BytesReference> documents, MediaType documentXContentType) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
        if (documents == null) {
            throw new IllegalArgumentException("[document] is a required argument");
        }
        this.field = field;
        this.documents = documents;
        this.documentXContentType = Objects.requireNonNull(documentXContentType);
        indexedDocumentIndex = null;
        indexedDocumentId = null;
        indexedDocumentRouting = null;
        indexedDocumentPreference = null;
        indexedDocumentVersion = null;
        this.documentSupplier = null;
    }

    /**
     * Creates a percolator query builder instance for percolating a document in a remote index.
     *
     * @param field                     The field that contains the percolator query
     * @param indexedDocumentIndex      The index containing the document to percolate
     * @param indexedDocumentId         The id of the document to percolate
     * @param indexedDocumentRouting    The routing value for the document to percolate
     * @param indexedDocumentPreference The preference to use when fetching the document to percolate
     * @param indexedDocumentVersion    The expected version of the document to percolate
     */
    public PercolateQueryBuilder(
        String field,
        String indexedDocumentIndex,
        String indexedDocumentId,
        String indexedDocumentRouting,
        String indexedDocumentPreference,
        Long indexedDocumentVersion
    ) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
        if (indexedDocumentIndex == null) {
            throw new IllegalArgumentException("[index] is a required argument");
        }
        if (indexedDocumentId == null) {
            throw new IllegalArgumentException("[id] is a required argument");
        }
        this.field = field;
        this.indexedDocumentIndex = indexedDocumentIndex;
        this.indexedDocumentId = indexedDocumentId;
        this.indexedDocumentRouting = indexedDocumentRouting;
        this.indexedDocumentPreference = indexedDocumentPreference;
        this.indexedDocumentVersion = indexedDocumentVersion;
        this.documents = Collections.emptyList();
        this.documentXContentType = null;
        this.documentSupplier = null;
    }

    protected PercolateQueryBuilder(String field, Supplier<BytesReference> documentSupplier) {
        if (field == null) {
            throw new IllegalArgumentException("[field] is a required argument");
        }
        this.field = field;
        this.documents = Collections.emptyList();
        this.documentXContentType = null;
        this.documentSupplier = documentSupplier;
        indexedDocumentIndex = null;
        indexedDocumentId = null;
        indexedDocumentRouting = null;
        indexedDocumentPreference = null;
        indexedDocumentVersion = null;
    }

    /**
     * Read from a stream.
     */
    PercolateQueryBuilder(StreamInput in) throws IOException {
        super(in);
        field = in.readString();
        name = in.readOptionalString();
        if (in.getVersion().before(Version.V_2_0_0)) {
            String documentType = in.readOptionalString();
            if (documentType != null) {
                throw new IllegalStateException("documentType must be null");
            }
        }
        indexedDocumentIndex = in.readOptionalString();
        if (in.getVersion().before(Version.V_2_0_0)) {
            String indexedDocumentType = in.readOptionalString();
            if (indexedDocumentType != null) {
                throw new IllegalStateException("indexedDocumentType must be null");
            }
        }

        indexedDocumentId = in.readOptionalString();
        indexedDocumentRouting = in.readOptionalString();
        indexedDocumentPreference = in.readOptionalString();
        if (in.readBoolean()) {
            indexedDocumentVersion = in.readVLong();
        } else {
            indexedDocumentVersion = null;
        }
        documents = in.readList(StreamInput::readBytesReference);
        if (documents.isEmpty() == false) {
            if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
                documentXContentType = in.readMediaType();
            } else {
                documentXContentType = in.readEnum(XContentType.class);
            }
        } else {
            documentXContentType = null;
        }
        documentSupplier = null;
    }

    /**
     * Sets the name used for identification purposes in <code>_percolator_document_slot</code> response field
     * when multiple percolate queries have been specified in the main query.
     */
    public PercolateQueryBuilder setName(String name) {
        this.name = name;
        return this;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (documentSupplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(field);
        out.writeOptionalString(name);
        if (out.getVersion().before(Version.V_2_0_0)) {
            // In 7x, typeless percolate queries are represented by null documentType values
            out.writeOptionalString(null);
        }
        out.writeOptionalString(indexedDocumentIndex);
        if (out.getVersion().before(Version.V_2_0_0)) {
            // In 7x, typeless percolate queries are represented by null indexedDocumentType values
            out.writeOptionalString(null);
        }
        out.writeOptionalString(indexedDocumentId);
        out.writeOptionalString(indexedDocumentRouting);
        out.writeOptionalString(indexedDocumentPreference);
        if (indexedDocumentVersion != null) {
            out.writeBoolean(true);
            out.writeVLong(indexedDocumentVersion);
        } else {
            out.writeBoolean(false);
        }
        out.writeVInt(documents.size());
        for (BytesReference document : documents) {
            out.writeBytesReference(document);
        }
        if (documents.isEmpty() == false) {
            if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
                documentXContentType.writeTo(out);
            } else {
                out.writeEnum((XContentType) documentXContentType);
            }
        }
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(QUERY_FIELD.getPreferredName(), field);
        if (name != null) {
            builder.field(NAME_FIELD.getPreferredName(), name);
        }
        if (documents.isEmpty() == false) {
            builder.startArray(DOCUMENTS_FIELD.getPreferredName());
            for (BytesReference document : documents) {
                try (
                    XContentParser parser = XContentHelper.createParser(
                        NamedXContentRegistry.EMPTY,
                        LoggingDeprecationHandler.INSTANCE,
                        document
                    )
                ) {
                    parser.nextToken();
                    builder.generator().copyCurrentStructure(parser);
                }
            }
            builder.endArray();
        }
        if (indexedDocumentIndex != null || indexedDocumentId != null) {
            if (indexedDocumentIndex != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_INDEX.getPreferredName(), indexedDocumentIndex);
            }
            if (indexedDocumentId != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_ID.getPreferredName(), indexedDocumentId);
            }
            if (indexedDocumentRouting != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_ROUTING.getPreferredName(), indexedDocumentRouting);
            }
            if (indexedDocumentPreference != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_PREFERENCE.getPreferredName(), indexedDocumentPreference);
            }
            if (indexedDocumentVersion != null) {
                builder.field(INDEXED_DOCUMENT_FIELD_VERSION.getPreferredName(), indexedDocumentVersion);
            }
        }
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    private static final ConstructingObjectParser<PercolateQueryBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        String field = (String) args[0];
        BytesReference document = (BytesReference) args[1];
        @SuppressWarnings("unchecked")
        List<BytesReference> documents = (List<BytesReference>) args[2];
        String indexedDocId = (String) args[3];
        String indexedDocIndex = (String) args[4];
        String indexDocRouting = (String) args[5];
        String indexDocPreference = (String) args[6];
        Long indexedDocVersion = (Long) args[7];
        if (indexedDocId != null) {
            return new PercolateQueryBuilder(field, indexedDocIndex, indexedDocId, indexDocRouting, indexDocPreference, indexedDocVersion);
        } else if (document != null) {
            return new PercolateQueryBuilder(field, Collections.singletonList(document), MediaTypeRegistry.JSON);
        } else {
            return new PercolateQueryBuilder(field, documents, MediaTypeRegistry.JSON);
        }
    });
    static {
        PARSER.declareString(constructorArg(), QUERY_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> parseDocument(p), DOCUMENT_FIELD);
        PARSER.declareObjectArray(optionalConstructorArg(), (p, c) -> parseDocument(p), DOCUMENTS_FIELD);
        PARSER.declareString(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_ID);
        PARSER.declareString(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_INDEX);
        PARSER.declareString(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_ROUTING);
        PARSER.declareString(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_PREFERENCE);
        PARSER.declareLong(optionalConstructorArg(), INDEXED_DOCUMENT_FIELD_VERSION);
        PARSER.declareString(PercolateQueryBuilder::setName, NAME_FIELD);
        PARSER.declareString(PercolateQueryBuilder::queryName, AbstractQueryBuilder.NAME_FIELD);
        PARSER.declareFloat(PercolateQueryBuilder::boost, BOOST_FIELD);
        PARSER.declareRequiredFieldSet(
            DOCUMENT_FIELD.getPreferredName(),
            DOCUMENTS_FIELD.getPreferredName(),
            INDEXED_DOCUMENT_FIELD_ID.getPreferredName()
        );
        PARSER.declareExclusiveFieldSet(
            DOCUMENT_FIELD.getPreferredName(),
            DOCUMENTS_FIELD.getPreferredName(),
            INDEXED_DOCUMENT_FIELD_ID.getPreferredName()
        );
    }

    private static BytesReference parseDocument(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            builder.flush();
            return BytesReference.bytes(builder);
        }
    }

    public static PercolateQueryBuilder fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    protected boolean doEquals(PercolateQueryBuilder other) {
        return Objects.equals(field, other.field)
            && Objects.equals(documents, other.documents)
            && Objects.equals(indexedDocumentIndex, other.indexedDocumentIndex)
            && Objects.equals(documentSupplier, other.documentSupplier)
            && Objects.equals(indexedDocumentId, other.indexedDocumentId);

    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, documents, indexedDocumentIndex, indexedDocumentId, documentSupplier);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) {
        if (documents.isEmpty() == false) {
            return this;
        } else if (documentSupplier != null) {
            final BytesReference source = documentSupplier.get();
            if (source == null) {
                return this; // not executed yet
            } else {
                PercolateQueryBuilder rewritten = new PercolateQueryBuilder(
                    field,
                    Collections.singletonList(source),
                    MediaTypeRegistry.xContentType(source)
                );
                if (name != null) {
                    rewritten.setName(name);
                }
                return rewritten;
            }
        }
        GetRequest getRequest = new GetRequest(indexedDocumentIndex, indexedDocumentId);
        getRequest.preference("_local");
        getRequest.routing(indexedDocumentRouting);
        getRequest.preference(indexedDocumentPreference);
        if (indexedDocumentVersion != null) {
            getRequest.version(indexedDocumentVersion);
        }
        SetOnce<BytesReference> documentSupplier = new SetOnce<>();
        queryShardContext.registerAsyncAction((client, listener) -> {
            client.get(getRequest, ActionListener.wrap(getResponse -> {
                if (getResponse.isExists() == false) {
                    throw new ResourceNotFoundException(
                        "indexed document [{}/{}] couldn't be found",
                        indexedDocumentIndex,
                        indexedDocumentId
                    );
                }
                if (getResponse.isSourceEmpty()) {
                    throw new IllegalArgumentException(
                        "indexed document [" + indexedDocumentIndex + "/" + indexedDocumentId + "] source disabled"
                    );
                }
                documentSupplier.set(getResponse.getSourceAsBytesRef());
                listener.onResponse(null);
            }, listener::onFailure));
        });

        PercolateQueryBuilder rewritten = new PercolateQueryBuilder(field, documentSupplier::get);
        if (name != null) {
            rewritten.setName(name);
        }
        return rewritten;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        if (context.allowExpensiveQueries() == false) {
            throw new OpenSearchException(
                "[percolate] queries cannot be executed when '" + ALLOW_EXPENSIVE_QUERIES.getKey() + "' is set to false."
            );
        }

        // Call nowInMillis() so that this query becomes un-cacheable since we
        // can't be sure that it doesn't use now or scripts
        context.nowInMillis();
        if (indexedDocumentIndex != null || indexedDocumentId != null || documentSupplier != null) {
            throw new IllegalStateException("query builder must be rewritten first");
        }

        if (documents.isEmpty()) {
            throw new IllegalStateException("no document to percolate");
        }

        MappedFieldType fieldType = context.fieldMapper(field);
        if (fieldType == null) {
            throw new QueryShardException(context, "field [" + field + "] does not exist");
        }

        if (!(fieldType.unwrap() instanceof PercolatorFieldMapper.PercolatorFieldType)) {
            throw new QueryShardException(
                context,
                "expected field [" + field + "] to be of type [percolator], but is of type [" + fieldType.typeName() + "]"
            );
        }

        final List<ParsedDocument> docs = new ArrayList<>();
        final DocumentMapper docMapper;
        final MapperService mapperService = context.getMapperService();
        String type = mapperService.documentMapper().type();
        docMapper = mapperService.documentMapper();
        for (BytesReference document : documents) {
            docs.add(docMapper.parse(new SourceToParse(context.index().getName(), "_temp_id", document, documentXContentType)));
        }

        FieldNameAnalyzer fieldNameAnalyzer = (FieldNameAnalyzer) docMapper.mappers().indexAnalyzer();
        // Need to this custom impl because FieldNameAnalyzer is strict and the percolator sometimes isn't when
        // 'index.percolator.map_unmapped_fields_as_string' is enabled:
        Analyzer analyzer = new DelegatingAnalyzerWrapper(Analyzer.PER_FIELD_REUSE_STRATEGY) {
            @Override
            protected Analyzer getWrappedAnalyzer(String fieldName) {
                Analyzer analyzer = fieldNameAnalyzer.analyzers().get(fieldName);
                if (analyzer != null) {
                    return analyzer;
                } else {
                    return context.getIndexAnalyzers().getDefaultIndexAnalyzer();
                }
            }
        };
        final IndexSearcher docSearcher;
        final boolean excludeNestedDocuments;
        if (docs.size() > 1 || docs.get(0).docs().size() > 1) {
            assert docs.size() != 1 || docMapper.hasNestedObjects();
            docSearcher = createMultiDocumentSearcher(analyzer, docs);
            excludeNestedDocuments = docMapper.hasNestedObjects()
                && docs.stream().map(ParsedDocument::docs).mapToInt(List::size).anyMatch(size -> size > 1);
        } else {
            MemoryIndex memoryIndex = MemoryIndex.fromDocument(docs.get(0).rootDoc(), analyzer, true, false);
            docSearcher = memoryIndex.createSearcher();
            docSearcher.setQueryCache(null);
            excludeNestedDocuments = false;
        }

        PercolatorFieldMapper.PercolatorFieldType pft = (PercolatorFieldMapper.PercolatorFieldType) fieldType;
        String name = this.name != null ? this.name : pft.name();
        QueryShardContext percolateShardContext = wrap(context);
        PercolatorFieldMapper.configureContext(percolateShardContext, pft.mapUnmappedFieldsAsText);
        PercolateQuery.QueryStore queryStore = createStore(pft.queryBuilderField, percolateShardContext);

        return pft.percolateQuery(name, queryStore, documents, docSearcher, excludeNestedDocuments, context.indexVersionCreated());
    }

    public String getField() {
        return field;
    }

    public List<BytesReference> getDocuments() {
        return documents;
    }

    // pkg-private for testing
    MediaType getXContentType() {
        return documentXContentType;
    }

    public String getQueryName() {
        return name;
    }

    static IndexSearcher createMultiDocumentSearcher(Analyzer analyzer, Collection<ParsedDocument> docs) {
        Directory directory = new ByteBuffersDirectory();
        try (IndexWriter indexWriter = new IndexWriter(directory, new IndexWriterConfig(analyzer))) {
            // Indexing in order here, so that the user provided order matches with the docid sequencing:
            Iterable<ParseContext.Document> iterable = () -> docs.stream().map(ParsedDocument::docs).flatMap(Collection::stream).iterator();
            indexWriter.addDocuments(iterable);

            DirectoryReader directoryReader = DirectoryReader.open(indexWriter);
            assert directoryReader.leaves().size() == 1 : "Expected single leaf, but got [" + directoryReader.leaves().size() + "]";
            final IndexSearcher slowSearcher = new IndexSearcher(directoryReader);
            slowSearcher.setQueryCache(null);
            return slowSearcher;
        } catch (IOException e) {
            throw new OpenSearchException("Failed to create index for percolator with nested document ", e);
        }
    }

    static PercolateQuery.QueryStore createStore(MappedFieldType queryBuilderFieldType, QueryShardContext context) {
        Version indexVersion = context.indexVersionCreated();
        NamedWriteableRegistry registry = context.getWriteableRegistry();
        return ctx -> {
            LeafReader leafReader = ctx.reader();
            BinaryDocValues binaryDocValues = leafReader.getBinaryDocValues(queryBuilderFieldType.name());
            if (binaryDocValues == null) {
                return docId -> null;
            }
            return docId -> {
                if (binaryDocValues.advanceExact(docId)) {
                    BytesRef qbSource = binaryDocValues.binaryValue();
                    try (InputStream in = new ByteArrayInputStream(qbSource.bytes, qbSource.offset, qbSource.length)) {
                        try (
                            StreamInput input = new NamedWriteableAwareStreamInput(
                                new InputStreamStreamInput(in, qbSource.length),
                                registry
                            )
                        ) {
                            input.setVersion(indexVersion);
                            // Query builder's content is stored via BinaryFieldMapper, which has a custom encoding
                            // to encode multiple binary values into a single binary doc values field.
                            // This is the reason we need to first need to read the number of values and
                            // then the length of the field value in bytes.
                            int numValues = input.readVInt();
                            assert numValues == 1;
                            int valueLength = input.readVInt();
                            assert valueLength > 0;
                            QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                            assert in.read() == -1;
                            queryBuilder = Rewriteable.rewrite(queryBuilder, context);
                            return queryBuilder.toQuery(context);
                        }
                    }
                } else {
                    return null;
                }
            };
        };
    }

    static QueryShardContext wrap(QueryShardContext shardContext) {
        return new QueryShardContext(shardContext) {

            @Override
            public IndexReader getIndexReader() {
                // The reader that matters in this context is not the reader of the shard but
                // the reader of the MemoryIndex. We just use `null` for simplicity.
                return null;
            }

            @Override
            public BitSetProducer bitsetFilter(Query query) {
                return context -> {
                    final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
                    final IndexSearcher searcher = new IndexSearcher(topLevelContext);
                    searcher.setQueryCache(null);
                    final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1f);
                    final Scorer s = weight.scorer(context);

                    if (s != null) {
                        return new BitDocIdSet(BitSet.of(s.iterator(), context.reader().maxDoc())).bits();
                    } else {
                        return null;
                    }
                };
            }

            @Override
            @SuppressWarnings("unchecked")
            public <IFD extends IndexFieldData<?>> IFD getForField(MappedFieldType fieldType) {
                IndexFieldData.Builder builder = fieldType.fielddataBuilder(
                    shardContext.getFullyQualifiedIndex().getName(),
                    shardContext::lookup
                );
                IndexFieldDataCache cache = new IndexFieldDataCache.None();
                CircuitBreakerService circuitBreaker = new NoneCircuitBreakerService();
                return (IFD) builder.build(cache, circuitBreaker);
            }
        };
    }
}
