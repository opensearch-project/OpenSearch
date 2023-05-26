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
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.junit.After;
import org.junit.Before;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.Mapping;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

import static org.opensearch.index.translog.TranslogDeletionPolicies.createTranslogDeletionPolicy;

public abstract class TranslogManagerTestCase extends OpenSearchTestCase {

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
        return new LocalTranslog(
            translogConfig,
            translogUUID,
            createTranslogDeletionPolicy(INDEX_SETTINGS),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            primaryTermSupplier,
            seqNo -> {}
        );
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
}
