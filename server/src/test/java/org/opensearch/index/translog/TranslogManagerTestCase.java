/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;


import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.common.Nullable;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.Mapping;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.seqno.ReplicationTracker;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.store.Store;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.test.DummyShardLock;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static org.opensearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;

public class TranslogManagerTestCase extends OpenSearchTestCase {

    protected final ShardId shardId = new ShardId(new Index("index", "_na_"), 0);
    protected final AllocationId allocationId = AllocationId.newInitializing();
    protected static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
    private AtomicLong globalCheckpoint;
    protected ThreadPool threadPool;
    protected final PrimaryTermSupplier primaryTerm = new PrimaryTermSupplier(1L);

    protected IndexSettings defaultSettings;
    protected String codecName;
    protected Path primaryTranslogDir;
    protected String translogUUID;

    protected static final BytesArray SOURCE = bytesArray("{}");
    protected static final BytesReference B_1 = new BytesArray(new byte[] { 1 });

    protected Translog createTranslog(LongSupplier primaryTermSupplier) throws IOException {
        return createTranslog(primaryTranslogDir, primaryTermSupplier);
    }

    protected Translog createTranslog(Path translogPath, LongSupplier primaryTermSupplier) throws IOException {
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, INDEX_SETTINGS, BigArrays.NON_RECYCLING_INSTANCE);
        String translogUUID = Translog.createEmptyTranslog(
            translogPath,
            SequenceNumbers.NO_OPS_PERFORMED,
            shardId,
            primaryTermSupplier.getAsLong()
        );
        return new Translog(
            translogConfig,
            translogUUID,
            createTranslogDeletionPolicy(INDEX_SETTINGS),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            primaryTermSupplier,
            seqNo -> {}
        );
    }

    protected Store createStore() throws IOException {
        return createStore(newDirectory());
    }

    protected Store createStore(final Directory directory) throws IOException {
        return createStore(INDEX_SETTINGS, directory);
    }

    protected Store createStore(final IndexSettings indexSettings, final Directory directory) throws IOException {
        return new Store(shardId, indexSettings, directory, new DummyShardLock(shardId));
    }

    private String create(Path path) throws IOException {
        globalCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        return Translog.createEmptyTranslog(path, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        primaryTerm.set(randomIntBetween(1, 100));
        defaultSettings = IndexSettingsModule.newIndexSettings("test", indexSettings());
        threadPool = new TestThreadPool(getClass().getName());
        primaryTranslogDir = createTempDir("translog-primary");
        translogUUID = create(primaryTranslogDir);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(() -> terminate(threadPool));
    }

    /**
     * Reads all engine operations that have been processed by the engine from Lucene index.
     * The returned operations are sorted and de-duplicated, thus each sequence number will be have at most one operation.
     */
    public static List<Translog.Operation> readAllOperationsInLucene(Engine engine) throws IOException {
        final List<Translog.Operation> operations = new ArrayList<>();
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", 0, Long.MAX_VALUE, false, randomBoolean())) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                operations.add(op);
            }
        }
        return operations;
    }

    /**
     * Reads all engine operations that have been processed by the engine from Lucene index/Translog based on source.
     */
    public static List<Translog.Operation> readAllOperationsBasedOnSource(Engine engine) throws IOException {
        final List<Translog.Operation> operations = new ArrayList<>();
        try (Translog.Snapshot snapshot = engine.newChangesSnapshot("test", 0, Long.MAX_VALUE, false, randomBoolean())) {
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                operations.add(op);
            }
        }
        return operations;
    }

    protected Settings indexSettings() {
        // TODO randomize more settings
        return Settings.builder()
            .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h") // make sure this doesn't kick in on us
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(
                IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.getKey(),
                between(10, 10 * IndexSettings.MAX_REFRESH_LISTENERS_PER_SHARD.get(Settings.EMPTY))
            )
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000))
            .build();
    }


    public static final class PrimaryTermSupplier implements LongSupplier {
        private final AtomicLong term;

        PrimaryTermSupplier(long initialTerm) {
            this.term = new AtomicLong(initialTerm);
        }

        public long get() {
            return term.get();
        }

        public void set(long newTerm) {
            this.term.set(newTerm);
        }

        @Override
        public long getAsLong() {
            return get();
        }
    }

    protected static ParsedDocument testParsedDocument(
        String id,
        String routing,
        ParseContext.Document document,
        BytesReference source,
        Mapping mappingUpdate
    ) {
        return testParsedDocument(id, routing, document, source, mappingUpdate, false);
    }

    protected static ParsedDocument testParsedDocument(
        String id,
        String routing,
        ParseContext.Document document,
        BytesReference source,
        Mapping mappingUpdate,
        boolean recoverySource
    ) {
        Field uidField = new Field("_id", Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        Field versionField = new NumericDocValuesField("_version", 0);
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        BytesRef ref = source.toBytesRef();
        if (recoverySource) {
            document.add(new StoredField(SourceFieldMapper.RECOVERY_SOURCE_NAME, ref.bytes, ref.offset, ref.length));
            document.add(new NumericDocValuesField(SourceFieldMapper.RECOVERY_SOURCE_NAME, 1));
        } else {
            document.add(new StoredField(SourceFieldMapper.NAME, ref.bytes, ref.offset, ref.length));
        }
        return new ParsedDocument(versionField, seqID, id, routing, List.of(document), source, XContentType.JSON, mappingUpdate);
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

    public static Term newUid(String id) {
        return new Term("_id", Uid.encodeId(id));
    }

    public static Term newUid(ParsedDocument doc) {
        return newUid(doc.id());
    }

    protected static BytesArray bytesArray(String string) {
        return new BytesArray(string.getBytes(Charset.defaultCharset()));
    }

    public EngineConfig config(
        IndexSettings indexSettings,
        Store store,
        Path translogPath,
        MergePolicy mergePolicy,
        ReferenceManager.RefreshListener refreshListener
    ) {
        return config(indexSettings, store, translogPath, mergePolicy, refreshListener, null, () -> SequenceNumbers.NO_OPS_PERFORMED);
    }

    public EngineConfig config(
        IndexSettings indexSettings,
        Store store,
        Path translogPath,
        MergePolicy mergePolicy,
        ReferenceManager.RefreshListener refreshListener,
        Sort indexSort,
        LongSupplier globalCheckpointSupplier
    ) {
        return config(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            refreshListener,
            indexSort,
            globalCheckpointSupplier,
            globalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY
        );
    }

    public EngineConfig config(
        final IndexSettings indexSettings,
        final Store store,
        final Path translogPath,
        final MergePolicy mergePolicy,
        final ReferenceManager.RefreshListener refreshListener,
        final Sort indexSort,
        final LongSupplier globalCheckpointSupplier,
        final Supplier<RetentionLeases> retentionLeasesSupplier
    ) {
        return config(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            refreshListener,
            null,
            indexSort,
            globalCheckpointSupplier,
            retentionLeasesSupplier,
            new NoneCircuitBreakerService()
        );
    }

    public EngineConfig config(
        IndexSettings indexSettings,
        Store store,
        Path translogPath,
        MergePolicy mergePolicy,
        ReferenceManager.RefreshListener externalRefreshListener,
        ReferenceManager.RefreshListener internalRefreshListener,
        Sort indexSort,
        @Nullable LongSupplier maybeGlobalCheckpointSupplier,
        CircuitBreakerService breakerService
    ) {
        return config(
            indexSettings,
            store,
            translogPath,
            mergePolicy,
            externalRefreshListener,
            internalRefreshListener,
            indexSort,
            maybeGlobalCheckpointSupplier,
            maybeGlobalCheckpointSupplier == null ? null : () -> RetentionLeases.EMPTY,
            breakerService
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
        final CircuitBreakerService breakerService
    ) {
        final IndexWriterConfig iwc = newIndexWriterConfig();
        final TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        final Engine.EventListener eventListener = new Engine.EventListener() {
        }; // we don't need to notify anybody in this test
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
                () -> SafeCommitInfo.EMPTY
            );
            globalCheckpointSupplier = replicationTracker;
            retentionLeasesSupplier = replicationTracker::getRetentionLeases;
        } else {
            assert maybeRetentionLeasesSupplier != null;
            globalCheckpointSupplier = maybeGlobalCheckpointSupplier;
            retentionLeasesSupplier = maybeRetentionLeasesSupplier;
        }
        return new EngineConfig(
            shardId,
            threadPool,
            indexSettings,
            null,
            store,
            mergePolicy,
            iwc.getAnalyzer(),
            iwc.getSimilarity(),
            new CodecService(null, logger),
            eventListener,
            IndexSearcher.getDefaultQueryCache(),
            IndexSearcher.getDefaultQueryCachingPolicy(),
            translogConfig,
            TimeValue.timeValueMinutes(5),
            extRefreshListenerList,
            intRefreshListenerList,
            indexSort,
            breakerService,
            globalCheckpointSupplier,
            retentionLeasesSupplier,
            primaryTerm,
            tombstoneDocSupplier()
        );
    }

    protected EngineConfig config(
        EngineConfig config,
        Store store,
        Path translogPath,
        EngineConfig.TombstoneDocSupplier tombstoneDocSupplier
    ) {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(config.getIndexSettings().getSettings())
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .build()
        );
        TranslogConfig translogConfig = new TranslogConfig(shardId, translogPath, indexSettings, BigArrays.NON_RECYCLING_INSTANCE);
        return new EngineConfig(
            config.getShardId(),
            config.getThreadPool(),
            indexSettings,
            config.getWarmer(),
            store,
            config.getMergePolicy(),
            config.getAnalyzer(),
            config.getSimilarity(),
            new CodecService(null, logger),
            config.getEventListener(),
            config.getQueryCache(),
            config.getQueryCachingPolicy(),
            translogConfig,
            config.getFlushMergesAfter(),
            config.getExternalRefreshListener(),
            config.getInternalRefreshListener(),
            config.getIndexSort(),
            config.getCircuitBreakerService(),
            config.getGlobalCheckpointSupplier(),
            config.retentionLeasesSupplier(),
            config.getPrimaryTermSupplier(),
            tombstoneDocSupplier
        );
    }

    /**
     * Creates a tombstone document that only includes uid, seq#, term and version fields.
     */
    public static EngineConfig.TombstoneDocSupplier tombstoneDocSupplier() {
        return new EngineConfig.TombstoneDocSupplier() {
            @Override
            public ParsedDocument newDeleteTombstoneDoc(String id) {
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
                return new ParsedDocument(
                    versionField,
                    seqID,
                    id,
                    null,
                    Collections.singletonList(doc),
                    new BytesArray("{}"),
                    XContentType.JSON,
                    null
                );
            }

            @Override
            public ParsedDocument newNoopTombstoneDoc(String reason) {
                final ParseContext.Document doc = new ParseContext.Document();
                SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
                doc.add(seqID.seqNo);
                doc.add(seqID.seqNoDocValue);
                doc.add(seqID.primaryTerm);
                seqID.tombstoneField.setLongValue(1);
                doc.add(seqID.tombstoneField);
                Field versionField = new NumericDocValuesField(VersionFieldMapper.NAME, 0);
                doc.add(versionField);
                BytesRef byteRef = new BytesRef(reason);
                doc.add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
                return new ParsedDocument(versionField, seqID, null, null, Collections.singletonList(doc), null, XContentType.JSON, null);
            }
        };
    }
}

