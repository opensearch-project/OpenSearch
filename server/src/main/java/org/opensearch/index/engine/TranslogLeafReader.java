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

package org.opensearch.index.engine;

import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafMetaData;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.TermVectors;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.mapper.RoutingFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.SourceToParse;
import org.opensearch.index.mapper.Uid;
import org.opensearch.index.translog.Translog;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import static org.apache.lucene.index.DirectoryReader.open;

/**
 * Internal class that mocks a single doc read from the transaction log as a leaf reader.
 *
 * @opensearch.internal
 */
public final class TranslogLeafReader extends LeafReader {

    private final Translog.Index operation;
    private final EngineConfig engineConfig;
    private volatile LeafReader inMemoryIndexReader;
    private static final FieldInfo FAKE_SOURCE_FIELD = new FieldInfo(
        SourceFieldMapper.NAME,
        1,
        false,
        false,
        false,
        IndexOptions.NONE,
        DocValuesType.NONE,
        DocValuesSkipIndexType.NONE,
        -1,
        Collections.emptyMap(),
        0,
        0,
        0,
        0,
        VectorEncoding.FLOAT32,
        VectorSimilarityFunction.EUCLIDEAN,
        false,
        false
    );
    private static final FieldInfo FAKE_ROUTING_FIELD = new FieldInfo(
        RoutingFieldMapper.NAME,
        2,
        false,
        false,
        false,
        IndexOptions.NONE,
        DocValuesType.NONE,
        DocValuesSkipIndexType.NONE,
        -1,
        Collections.emptyMap(),
        0,
        0,
        0,
        0,
        VectorEncoding.FLOAT32,
        VectorSimilarityFunction.EUCLIDEAN,
        false,
        false
    );
    private static final FieldInfo FAKE_ID_FIELD = new FieldInfo(
        IdFieldMapper.NAME,
        3,
        false,
        false,
        false,
        IndexOptions.NONE,
        DocValuesType.NONE,
        DocValuesSkipIndexType.NONE,
        -1,
        Collections.emptyMap(),
        0,
        0,
        0,
        0,
        VectorEncoding.FLOAT32,
        VectorSimilarityFunction.EUCLIDEAN,
        false,
        false
    );
    public static Set<String> ALL_FIELD_NAMES = Sets.newHashSet(FAKE_SOURCE_FIELD.name, FAKE_ROUTING_FIELD.name, FAKE_ID_FIELD.name);

    public TranslogLeafReader(Translog.Index operation, EngineConfig engineConfig) {
        this.operation = operation;
        this.engineConfig = engineConfig;
    }

    private LeafReader getInMemoryIndexReader() throws IOException {
        if (inMemoryIndexReader == null) {
            inMemoryIndexReader = createInMemoryIndexReader(operation, engineConfig);
        }
        return inMemoryIndexReader;
    }

    public static LeafReader createInMemoryIndexReader(Translog.Index operation, EngineConfig engineConfig) throws IOException {
        boolean success = false;
        final Directory directory = new ByteBuffersDirectory();
        try {
            SourceToParse sourceToParse = new SourceToParse(
                engineConfig.getIndexSettings().getIndex().getName(),
                operation.id(),
                operation.source(),
                MediaTypeRegistry.xContentType(operation.source()),
                operation.routing()
            );
            ParsedDocument parsedDocument = engineConfig.getDocumentMapperForTypeSupplier().get().getDocumentMapper().parse(sourceToParse);
            parsedDocument.updateSeqID(operation.seqNo(), operation.primaryTerm());
            parsedDocument.version().setLongValue(operation.version());
            final IndexWriterConfig iwc = new IndexWriterConfig(engineConfig.getAnalyzer());
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            iwc.setCodec(engineConfig.getCodec());
            IndexWriter indexWriter = new IndexWriter(directory, iwc);
            indexWriter.addDocuments(parsedDocument.docs());
            final DirectoryReader directoryReader = open(indexWriter);
            if (directoryReader.leaves().size() != 1
                || directoryReader.leaves().get(0).reader().numDocs() != parsedDocument.docs().size()) {
                throw new IllegalStateException(
                    "Expected a single segment with "
                        + parsedDocument.docs().size()
                        + " documents, but ["
                        + directoryReader.leaves().size()
                        + " segments with "
                        + directoryReader.leaves().get(0).reader().numDocs()
                        + " documents"
                );
            }
            LeafReader leafReader = directoryReader.leaves().get(0).reader();
            LeafReader sequentialLeafReader = new SequentialStoredFieldsLeafReader(leafReader) {
                @Override
                protected void doClose() throws IOException {
                    IOUtils.close(super::doClose, directory);
                }

                @Override
                public CacheHelper getCoreCacheHelper() {
                    return leafReader.getCoreCacheHelper();
                }

                @Override
                public CacheHelper getReaderCacheHelper() {
                    return leafReader.getReaderCacheHelper();
                }

                @Override
                public StoredFieldsReader getSequentialStoredFieldsReader() {
                    return Lucene.segmentReader(leafReader).getFieldsReader().getMergeInstance();
                }

                @Override
                protected StoredFieldsReader doGetSequentialStoredFieldsReader(StoredFieldsReader reader) {
                    return reader;
                }
            };
            success = true;
            return sequentialLeafReader;
        } finally {
            if (!success) {
                IOUtils.closeWhileHandlingException(directory);
            }
        }
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Terms terms(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NumericDocValues getNumericDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public BinaryDocValues getBinaryDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedDocValues getSortedDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedNumericDocValues getSortedNumericDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedSetDocValues getSortedSetDocValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public NumericDocValues getNormValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public FieldInfos getFieldInfos() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Bits getLiveDocs() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PointValues getPointValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void checkIntegrity() {

    }

    @Override
    public LeafMetaData getMetaData() {
        throw new UnsupportedOperationException();
    }

    @Override
    public TermVectors termVectors() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public int numDocs() {
        return 1;
    }

    @Override
    public int maxDoc() {
        return 1;
    }

    @Override
    public StoredFields storedFields() throws IOException {
        return new StoredFields() {
            @Override
            public void document(int docID, StoredFieldVisitor visitor) throws IOException {
                if (docID != 0) {
                    throw new IllegalArgumentException("no such doc ID " + docID);
                }
                if (visitor.needsField(FAKE_SOURCE_FIELD) == StoredFieldVisitor.Status.YES) {
                    if (engineConfig.getIndexSettings().isDerivedSourceEnabled()
                        && engineConfig.getIndexSettings().isDerivedSourceEnabledForTranslog()) {
                        LeafReader leafReader = getInMemoryIndexReader();
                        assert leafReader != null && leafReader.leaves().size() == 1;
                        visitor.binaryField(
                            FAKE_SOURCE_FIELD,
                            engineConfig.getDocumentMapperForTypeSupplier()
                                .get()
                                .getDocumentMapper()
                                .root()
                                .deriveSource(leafReader, docID)
                                .toBytesRef().bytes
                        );
                    } else {
                        assert operation.source().toBytesRef().offset == 0;
                        assert operation.source().toBytesRef().length == operation.source().toBytesRef().bytes.length;
                        visitor.binaryField(FAKE_SOURCE_FIELD, operation.source().toBytesRef().bytes);
                    }
                }
                if (operation.routing() != null && visitor.needsField(FAKE_ROUTING_FIELD) == StoredFieldVisitor.Status.YES) {
                    visitor.stringField(FAKE_ROUTING_FIELD, operation.routing());
                }
                if (visitor.needsField(FAKE_ID_FIELD) == StoredFieldVisitor.Status.YES) {
                    BytesRef bytesRef = Uid.encodeId(operation.id());
                    final byte[] id = new byte[bytesRef.length];
                    System.arraycopy(bytesRef.bytes, bytesRef.offset, id, 0, bytesRef.length);
                    visitor.binaryField(FAKE_ID_FIELD, id);
                }
            }
        };
    }

    @Override
    protected void doClose() {

    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        throw new UnsupportedOperationException();
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void searchNearestVectors(String field, byte[] target, KnnCollector k, AcceptDocs acceptDocs) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void searchNearestVectors(String field, float[] target, KnnCollector k, AcceptDocs acceptDocs) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public DocValuesSkipper getDocValuesSkipper(String field) throws IOException {
        throw new UnsupportedOperationException();
    }
}
