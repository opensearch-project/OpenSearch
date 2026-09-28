/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.Index;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.DocumentMapper;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TranslogLeafReaderTests extends OpenSearchTestCase {

    private EngineConfig engineConfig;
    private Translog.Index operation;
    private TranslogLeafReader translogLeafReader;
    private DocumentMapper documentMapper;
    private DocumentMapperForType documentMapperForType;
    private Index index;
    private BytesReference source;
    private IndexSettings defaultIndexSettings;

    @Before
    public void setup() throws Exception {
        index = new Index("test", "_uuid");
        final IndexMetadata defaultIndexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        defaultIndexSettings = IndexSettingsModule.newIndexSettings("test", defaultIndexMetadata.getSettings());
        documentMapper = mock(DocumentMapper.class);

        documentMapperForType = mock(DocumentMapperForType.class);
        when(documentMapperForType.getDocumentMapper()).thenReturn(documentMapper);

        engineConfig = new EngineConfig.Builder().indexSettings(defaultIndexSettings)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .codecService(new CodecService(null, defaultIndexSettings, logger, List.of()))
            .documentMapperForTypeSupplier(() -> documentMapperForType)
            .build();

        // Setup basic operation
        source = new BytesArray("{\"field\":1}");
        operation = new Translog.Index("test", 1L, 1L, 1L, source.toBytesRef().bytes, "routing", 1);

        // Initialize the reader
        translogLeafReader = new TranslogLeafReader(operation, engineConfig);
    }

    public void testBasicProperties() {
        assertEquals(1, translogLeafReader.numDocs());
        assertEquals(1, translogLeafReader.maxDoc());
    }

    public void testStoredFieldsAccess() throws IOException {
        StoredFields storedFields = translogLeafReader.storedFields();
        assertNotNull(storedFields);

        // Test accessing invalid document ID
        expectThrows(IllegalArgumentException.class, () -> {
            storedFields.document(1, new FieldsVisitor(true) {
            });
        });
    }

    public void testSourceFieldAccess() throws IOException {
        StoredFields storedFields = translogLeafReader.storedFields();

        final BytesReference[] sourceRef = new BytesReference[1];
        StoredFieldVisitor visitor = new StoredFieldVisitor() {
            @Override
            public void binaryField(FieldInfo fieldInfo, byte[] value) {
                if (fieldInfo.name.equals(SourceFieldMapper.NAME)) {
                    sourceRef[0] = new BytesArray(value);
                }
            }

            @Override
            public Status needsField(FieldInfo fieldInfo) {
                return fieldInfo.name.equals(SourceFieldMapper.NAME) ? Status.YES : Status.NO;
            }
        };

        storedFields.document(0, visitor);
        assertNotNull(sourceRef[0]);
        assertEquals(operation.source(), sourceRef[0]);
    }

    public void testIdFieldAccess() throws IOException {
        StoredFields storedFields = translogLeafReader.storedFields();

        final BytesReference[] idRef = new BytesReference[1];
        StoredFieldVisitor visitor = new StoredFieldVisitor() {
            @Override
            public void binaryField(FieldInfo fieldInfo, byte[] value) {
                if (fieldInfo.name.equals(IdFieldMapper.NAME)) {
                    idRef[0] = new BytesArray(Uid.decodeId(value));
                }
            }

            @Override
            public Status needsField(FieldInfo fieldInfo) {
                return fieldInfo.name.equals(IdFieldMapper.NAME) ? Status.YES : Status.NO;
            }
        };

        storedFields.document(0, visitor);
        assertNotNull(idRef[0]);
        assertEquals(operation.id(), idRef[0].utf8ToString());
    }

    public void testRoutingFieldAccess() throws IOException {
        StoredFields storedFields = translogLeafReader.storedFields();

        final String[] routingVal = new String[1];
        StoredFieldVisitor visitor = new StoredFieldVisitor() {
            @Override
            public void stringField(FieldInfo fieldInfo, String value) {
                if (fieldInfo.name.equals(RoutingFieldMapper.NAME)) {
                    routingVal[0] = value;
                }
            }

            @Override
            public Status needsField(FieldInfo fieldInfo) {
                return fieldInfo.name.equals(RoutingFieldMapper.NAME) ? Status.YES : Status.NO;
            }
        };

        storedFields.document(0, visitor);
        assertNotNull(routingVal[0]);
        assertEquals(operation.routing(), routingVal[0]);
    }

    public void testDerivedSourceFieldsUsingDerivedSource() throws IOException {
        // Setup mapper service with derived source enabled
        Settings derivedSourceSettings = Settings.builder()
            .put(defaultIndexSettings.getSettings())
            .put(IndexSettings.INDEX_DERIVED_SOURCE_SETTING.getKey(), true)
            .build();
        IndexMetadata derivedMetadata = IndexMetadata.builder("test").settings(derivedSourceSettings).build();
        IndexSettings derivedIndexSettings = new IndexSettings(derivedMetadata, Settings.EMPTY);

        engineConfig = new EngineConfig.Builder().indexSettings(derivedIndexSettings)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .codecService(new CodecService(null, derivedIndexSettings, logger, List.of()))
            .documentMapperForTypeSupplier(() -> documentMapperForType)
            .build();

        // Mock document mapper
        Document doc = new Document();
        doc.add(new StringField("field", "value", Field.Store.YES));
        ParsedDocument parsedDoc = new ParsedDocument(null, null, "1", null, null, source, MediaTypeRegistry.JSON, null);
        when(documentMapper.parse(any())).thenReturn(parsedDoc);

        StoredFields storedFields = translogLeafReader.storedFields();

        final BytesReference[] sourceRef = new BytesReference[1];
        StoredFieldVisitor visitor = new StoredFieldVisitor() {
            @Override
            public void binaryField(org.apache.lucene.index.FieldInfo fieldInfo, byte[] value) {
                if (fieldInfo.name.equals(SourceFieldMapper.NAME)) {
                    sourceRef[0] = new BytesArray(value);
                }
            }

            @Override
            public Status needsField(org.apache.lucene.index.FieldInfo fieldInfo) {
                return fieldInfo.name.equals(SourceFieldMapper.NAME) ? Status.YES : Status.NO;
            }
        };

        storedFields.document(0, visitor);
        assertNotNull(sourceRef[0]);
    }

    public void testDerivedSourceFieldsUsingSource() throws IOException {
        // Setup mapper service with derived source enabled
        Settings derivedSourceSettings = Settings.builder()
            .put(defaultIndexSettings.getSettings())
            .put(IndexSettings.INDEX_DERIVED_SOURCE_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_DERIVED_SOURCE_TRANSLOG_ENABLED_SETTING.getKey(), false)
            .build();
        IndexMetadata derivedMetadata = IndexMetadata.builder("test").settings(derivedSourceSettings).build();
        IndexSettings derivedIndexSettings = new IndexSettings(derivedMetadata, Settings.EMPTY);

        engineConfig = new EngineConfig.Builder().indexSettings(derivedIndexSettings)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .codecService(new CodecService(null, defaultIndexSettings, logger, List.of()))
            .documentMapperForTypeSupplier(() -> null)
            .build();

        translogLeafReader = new TranslogLeafReader(operation, engineConfig);

        StoredFields storedFields = translogLeafReader.storedFields();

        final BytesReference[] sourceRef = new BytesReference[1];
        StoredFieldVisitor visitor = new StoredFieldVisitor() {
            @Override
            public void binaryField(org.apache.lucene.index.FieldInfo fieldInfo, byte[] value) {
                if (fieldInfo.name.equals(SourceFieldMapper.NAME)) {
                    sourceRef[0] = new BytesArray(value);
                }
            }

            @Override
            public Status needsField(org.apache.lucene.index.FieldInfo fieldInfo) {
                return fieldInfo.name.equals(SourceFieldMapper.NAME) ? Status.YES : Status.NO;
            }
        };

        storedFields.document(0, visitor);
        assertNotNull(sourceRef[0]);
        assertEquals(operation.source(), sourceRef[0]);
    }

    public void testUnsupportedOperations() {
        // Test various unsupported operations
        expectThrows(UnsupportedOperationException.class, () -> translogLeafReader.terms("field"));
        expectThrows(UnsupportedOperationException.class, () -> translogLeafReader.getNumericDocValues("field"));
        expectThrows(UnsupportedOperationException.class, () -> translogLeafReader.getBinaryDocValues("field"));
        expectThrows(UnsupportedOperationException.class, () -> translogLeafReader.getSortedDocValues("field"));
        expectThrows(UnsupportedOperationException.class, () -> translogLeafReader.getPointValues("field"));
        expectThrows(UnsupportedOperationException.class, () -> translogLeafReader.getLiveDocs());
    }

    public void testCreateInMemoryIndexReader() throws IOException {
        // Setup test document
        Document doc = new Document();
        doc.add(new StringField("field", "value", Field.Store.YES));
        // Create SeqID for the ParsedDocument
        SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(1L);

        final ParseContext.Document document = new ParseContext.Document();
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);

        ParsedDocument parsedDoc = new ParsedDocument(
            new NumericDocValuesField("version", 1),
            seqID,
            operation.id(),
            null,
            Collections.singletonList(document),
            source,
            MediaTypeRegistry.JSON,
            null
        );

        // Mock necessary components
        when(documentMapper.parse(any())).thenReturn(parsedDoc);

        // Test creation of in-memory reader
        assertNotNull(TranslogLeafReader.createInMemoryIndexReader(operation, engineConfig));
    }
}
