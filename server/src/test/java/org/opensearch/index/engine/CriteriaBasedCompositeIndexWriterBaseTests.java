/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.common.CheckedBiFunction;
import org.opensearch.common.Nullable;
import org.opensearch.common.Randomness;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.core.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.BucketedCompositeDirectory;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.Mapping;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.opensearch.index.engine.Engine.Operation.Origin.PEER_RECOVERY;
import static org.opensearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.opensearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;

public class CriteriaBasedCompositeIndexWriterBaseTests extends OpenSearchTestCase {
    protected static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
    protected final EngineTestCase.PrimaryTermSupplier primaryTerm = new EngineTestCase.PrimaryTermSupplier(1L);
    protected Store store;
    protected Path primaryTranslogDir;
    protected ThreadPool threadPool;
    protected final ShardId shardId = new ShardId(new Index("index", "_na_"), 0);
    protected final AllocationId allocationId = AllocationId.newInitializing();
    protected final NumericDocValuesField softDeletesField = Lucene.newSoftDeletesField();
    public static final String DEFAULT_CRITERIA = "testGroupingCriteria";
    protected IndexWriterFactory indexWriterFactory;

    protected static ParsedDocument createParsedDoc(String id, String routing, String groupingCriteria) {
        return testParsedDocument(
            id,
            routing,
            testDocumentWithTextField(),
            new BytesArray("{ \"value\" : \"test\" }"),
            null,
            groupingCriteria
        );
    }

    protected static ParsedDocument testParsedDocument(
        String id,
        String routing,
        ParseContext.Document document,
        BytesReference source,
        Mapping mappingUpdate,
        String groupingCriteria
    ) {
        return testParsedDocument(id, routing, document, source, mappingUpdate, false, groupingCriteria);
    }

    protected static ParsedDocument testParsedDocument(
        String id,
        String routing,
        ParseContext.Document document,
        BytesReference source,
        Mapping mappingUpdate,
        boolean recoverySource,
        String groupingCriteria
    ) {
        Field uidField = new Field("_id", Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        document.setGroupingCriteria(groupingCriteria);
        if (source != null) {
            BytesRef ref = source.toBytesRef();
            if (recoverySource) {
                document.add(new StoredField(SourceFieldMapper.RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
                document.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_NAME, 1));
            } else {
                document.add(new StoredField(SourceFieldMapper.NAME, ref.bytes, ref.offset, ref.length));
            }
        }
        return new ParsedDocument(versionField, seqID, id, routing, Arrays.asList(document), source, MediaTypeRegistry.JSON, mappingUpdate);
    }

    protected static ParseContext.Document testDocumentWithTextField() {
        return testDocumentWithTextField("test");
    }

    protected static ParseContext.Document testDocumentWithTextField(String value) {
        ParseContext.Document document = testDocument();
        document.add(new TextField("value", value, Field.Store.YES));
        return document;
    }

    protected static ParseContext.Document testDocument() {
        return new ParseContext.Document();
    }

    protected Engine.Index indexForDoc(ParsedDocument doc) {
        return new Engine.Index(newUid(doc), primaryTerm.get(), doc);
    }

    protected static Term newUid(String id) {
        return new Term("_id", Uid.encodeId(id));
    }

    protected static Term newUid(ParsedDocument doc) {
        return newUid(doc.id());
    }

    protected IndexWriter createWriter() throws IOException {
        return createWriter(store.directory());
    }

    protected IndexWriter createWriter(Directory directory) throws IOException {
        try {
            return new IndexWriter(directory, newIndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD).setCommitOnClose(true));
        } catch (LockObtainFailedException ex) {
            logger.warn("could not lock IndexWriter", ex);
            throw ex;
        }
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        store = createStore(INDEX_SETTINGS, newDirectory());
        primaryTranslogDir = createTempDir("translog-primary");
        threadPool = new TestThreadPool(getClass().getName());
        primaryTranslogDir = createTempDir("translog-primary");
        indexWriterFactory = new NativeLuceneIndexWriterFactory();
    }

    protected Store createStore(final IndexSettings indexSettings, final Directory directory) throws IOException {
        final Path path = createTempDir().resolve(shardId.getIndex().getUUID()).resolve(String.valueOf(shardId.id()));
        final ShardPath shardPath = new ShardPath(false, path, path, shardId);
        return new Store(
            shardId,
            indexSettings,
            directory,
            new DummyShardLock(shardId),
            Store.OnClose.EMPTY,
            shardPath,
            new FsDirectoryFactory()
        );
    }

    public EngineConfig config() {
        return config(
            INDEX_SETTINGS,
            store,
            primaryTranslogDir,
            NoMergePolicy.INSTANCE,
            null,
            null,
            null,
            null,
            null,
            new NoneCircuitBreakerService(),
            null
        );
    }

    public EngineConfig config(
        final IndexSettings indexSettings,
        final Store store,
        final Path translogPath,
        final MergePolicy mergePolicy,
        final ReferenceManager.RefreshListener externalRefreshListener,
        final ReferenceManager.RefreshListener internalRefreshListener,
        final Sort indexSort,
        final @Nullable LongSupplier maybeGlobalCheckpointSupplier,
        final @Nullable Supplier<RetentionLeases> maybeRetentionLeasesSupplier,
        final CircuitBreakerService breakerService,
        final Engine.EventListener eventListener
    ) {
        final IndexWriterConfig iwc = newIndexWriterConfig();
        final TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            translogPath,
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            "",
            false
        );
        final List<ReferenceManager.RefreshListener> extRefreshListenerList = externalRefreshListener == null
            ? emptyList()
            : Collections.singletonList(externalRefreshListener);
        final List<ReferenceManager.RefreshListener> intRefreshListenerList = internalRefreshListener == null
            ? emptyList()
            : Collections.singletonList(internalRefreshListener);
        final LongSupplier globalCheckpointSupplier;
        final Supplier<RetentionLeases> retentionLeasesSupplier;
        if (maybeGlobalCheckpointSupplier == null) {
            assert maybeRetentionLeasesSupplier == null;
            final ReplicationTracker replicationTracker = new ReplicationTracker(
                shardId,
                allocationId.getId(),
                indexSettings,
                randomNonNegativeLong(),
                SequenceNumbers.NO_OPS_PERFORMED,
                update -> {},
                () -> 0L,
                (leases, listener) -> listener.onResponse(new ReplicationResponse()),
                () -> SafeCommitInfo.EMPTY,
                sId -> false
            );
            globalCheckpointSupplier = replicationTracker;
            retentionLeasesSupplier = replicationTracker::getRetentionLeases;
        } else {
            assert maybeRetentionLeasesSupplier != null;
            globalCheckpointSupplier = maybeGlobalCheckpointSupplier;
            retentionLeasesSupplier = maybeRetentionLeasesSupplier;
        }
        return new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .warmer(null)
            .store(store)
            .mergePolicy(mergePolicy)
            .analyzer(iwc.getAnalyzer())
            .similarity(iwc.getSimilarity())
            .codecService(new CodecService(null, indexSettings, logger))
            .eventListener(eventListener)
            .queryCache(IndexSearcher.getDefaultQueryCache())
            .queryCachingPolicy(IndexSearcher.getDefaultQueryCachingPolicy())
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(extRefreshListenerList)
            .internalRefreshListener(intRefreshListenerList)
            .indexSort(indexSort)
            .circuitBreakerService(breakerService)
            .globalCheckpointSupplier(globalCheckpointSupplier)
            .retentionLeasesSupplier(retentionLeasesSupplier)
            .primaryTermSupplier(primaryTerm)
            .build();
    }

    protected ParseContext.Document newDeleteTombstoneDoc(String id) {
        final ParseContext.Document doc = new ParseContext.Document();
        Field uidField = new Field(IdFieldMapper.NAME, Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        doc.add(uidField);
        Field versionField = new NumericDocValuesField(VersionFieldMapper.NAME, 0);
        doc.add(versionField);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        doc.add(seqID.seqNo);
        doc.add(seqID.seqNoDocValue);
        doc.add(seqID.primaryTerm);
        seqID.tombstoneField.setLongValue(1);
        doc.add(seqID.tombstoneField);
        doc.add(softDeletesField);
        return doc;
    }

    public List<Engine.Operation> generateHistoryOnReplica(
        int numOps,
        boolean allowGapInSeqNo,
        boolean allowDuplicate,
        boolean includeNestedDocs,
        Engine.Operation.TYPE opType
    ) throws Exception {
        long seqNo = 0;
        final int maxIdValue = randomInt(numOps * 2);
        final List<Engine.Operation> operations = new ArrayList<>(numOps);
        CheckedBiFunction<String, Integer, ParsedDocument, IOException> nestedParsedDocFactory = nestedParsedDocFactory();
        for (int i = 0; i < numOps; i++) {
            final String id = Integer.toString(randomInt(maxIdValue));
            final boolean isNestedDoc = includeNestedDocs && opType == Engine.Operation.TYPE.INDEX && randomBoolean();
            final int nestedValues = between(0, 3);
            final long startTime = threadPool.relativeTimeInNanos();
            final int copies = allowDuplicate && rarely() ? between(2, 4) : 1;
            for (int copy = 0; copy < copies; copy++) {
                final ParsedDocument doc = isNestedDoc
                    ? nestedParsedDocFactory.apply(id, nestedValues)
                    : createParsedDoc(id, null, DEFAULT_CRITERIA);
                switch (opType) {
                    case INDEX:
                        operations.add(
                            new Engine.Index(
                                EngineTestCase.newUid(doc),
                                doc,
                                seqNo,
                                primaryTerm.get(),
                                i,
                                null,
                                randomFrom(REPLICA, PEER_RECOVERY),
                                startTime,
                                -1,
                                true,
                                SequenceNumbers.UNASSIGNED_SEQ_NO,
                                0
                            )
                        );
                        break;
                    case DELETE:
                        operations.add(
                            new Engine.Delete(
                                doc.id(),
                                EngineTestCase.newUid(doc),
                                seqNo,
                                primaryTerm.get(),
                                i,
                                null,
                                randomFrom(REPLICA, PEER_RECOVERY),
                                startTime,
                                SequenceNumbers.UNASSIGNED_SEQ_NO,
                                0
                            )
                        );
                        break;
                    case NO_OP:
                        operations.add(
                            new Engine.NoOp(seqNo, primaryTerm.get(), randomFrom(REPLICA, PEER_RECOVERY), startTime, "test-" + i)
                        );
                        break;
                    default:
                        throw new IllegalStateException("Unknown operation type [" + opType + "]");
                }
            }
            seqNo++;
            if (allowGapInSeqNo && rarely()) {
                seqNo++;
            }
        }
        Randomness.shuffle(operations);
        return operations;
    }

    public static CheckedBiFunction<String, Integer, ParsedDocument, IOException> nestedParsedDocFactory() throws Exception {
        final MapperService mapperService = createMapperService();
        final String nestedMapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("type")
            .startObject("properties")
            .startObject("nested_field")
            .field("type", "nested")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();
        final DocumentMapper nestedMapper = mapperService.documentMapperParser().parse("type", new CompressedXContent(nestedMapping));
        return (docId, nestedFieldValues) -> {
            final XContentBuilder source = XContentFactory.jsonBuilder().startObject().field("field", "value");
            if (nestedFieldValues > 0) {
                XContentBuilder nestedField = source.startObject("nested_field");
                for (int i = 0; i < nestedFieldValues; i++) {
                    nestedField.field("field-" + i, "value-" + i);
                }
                source.endObject();
            }
            source.endObject();
            return nestedMapper.parse(new SourceToParse("test", docId, BytesReference.bytes(source), MediaTypeRegistry.JSON));
        };
    }

    public static MapperService createMapperService() throws IOException {
        return createMapperService("{\"properties\": {}}");
    }

    public static MapperService createMapperService(String mapping) throws IOException {
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
            .putMapping(mapping)
            .build();
        MapperService mapperService = MapperTestUtils.newMapperService(
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(),
            Settings.EMPTY,
            "test"
        );
        mapperService.merge(indexMetadata, MapperService.MergeReason.MAPPING_UPDATE);
        return mapperService;
    }

    protected CompositeIndexWriter.DisposableIndexWriter createChildWriterFactory(
        String criteria,
        CompositeIndexWriter.CriteriaBasedIndexWriterLookup lookup
    ) throws IOException {
        return new CompositeIndexWriter.DisposableIndexWriter(
            new IndexWriter(
                store.newTempDirectory(BucketedCompositeDirectory.CHILD_DIRECTORY_PREFIX + criteria + "_" + UUID.randomUUID()),
                newIndexWriterConfig().setSoftDeletesField(Lucene.SOFT_DELETES_FIELD).setCommitOnClose(true)
            ),
            lookup
        );
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(store, () -> terminate(threadPool));
    }

    public SoftDeletesPolicy newSoftDeletesPolicy() {
        final AtomicLong globalCheckpoint = new AtomicLong();
        final int extraRetainedOps = between(0, 100);
        final SoftDeletesPolicy softDeletesPolicy = new SoftDeletesPolicy(
            globalCheckpoint::get,
            NO_OPS_PERFORMED,
            extraRetainedOps,
            () -> RetentionLeases.EMPTY
        );

        return softDeletesPolicy;
    }
}
