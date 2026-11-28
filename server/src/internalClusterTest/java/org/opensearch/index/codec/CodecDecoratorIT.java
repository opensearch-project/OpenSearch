/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundDirectory;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.apache.lucene.util.BytesRef;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * The per-segment formats contributed by several plugins are composed within the same index: this is what
 * {@code k-NN} (compound files of the native engines, derived source) and {@code neural-search} (compound
 * files of the sparse engine) need in order to be usable together.
 */
@SuppressCodecs("*") // the codec of the index has to be one of the OpenSearch ones
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CodecDecoratorIT extends OpenSearchIntegTestCase {
    private static final String STORED_FIELDS_PREFIX = "decorated:";
    private static final String COMPOUND_ATTRIBUTE = "test.compound.decorator";

    /** Customizes the stored fields of the index, the way the derived source of {@code k-NN} does */
    public static class StoredFieldsEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<CodecDecorator> getCodecDecorator(MapperService mapperService, IndexSettings indexSettings) {
            return Optional.of(new PrefixingStoredFieldsDecorator());
        }
    }

    /** Customizes the compound format of the index, the way the native engines of the plugins do */
    public static class CompoundEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<CodecDecorator> getCodecDecorator(MapperService mapperService, IndexSettings indexSettings) {
            return Optional.of(new MarkingCompoundDecorator());
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StoredFieldsEnginePlugin.class);
        plugins.add(CompoundEnginePlugin.class);
        return plugins;
    }

    public void testPerSegmentFormatsOfSeveralPluginsAreComposed() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    // the compound decorator is only exercised when the segments are compound files
                    .put("index.compound_format", true)
            ).setMapping("value", "type=keyword")
        );

        for (int i = 0; i < 5; ++i) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource(Map.of("value", "value" + i)).get();
        }
        flushAndRefresh("test");
        client().admin().indices().prepareForceMerge("test").setMaxNumSegments(1).get();

        // the source goes through the stored fields format of the plugin, both while writing and while reading
        for (int i = 0; i < 5; ++i) {
            final GetResponse response = client().prepareGet("test", Integer.toString(i)).get();
            assertTrue(response.isExists());
            assertEquals(Map.of("value", "value" + i), response.getSourceAsMap());
        }

        final SearchResponse search = client().prepareSearch("test").setQuery(QueryBuilders.termQuery("value", "value3")).get();
        assertHitCount(search, 1);
        assertEquals(Map.of("value", "value3"), search.getHits().getHits()[0].getSourceAsMap());

        assertSegments();
    }

    private void assertSegments() throws Exception {
        int shards = 0;

        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            final IndexService indexService = indicesService.indexService(resolveIndex("test"));
            if (indexService == null) {
                continue;
            }

            for (final IndexShard shard : indexService) {
                try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                    for (final LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                        final SegmentInfo si = Lucene.segmentReader(leaf.reader()).getSegmentInfo().info;

                        // the segment records everything needed to reconstruct the formats while reading it
                        assertEquals(Decorated104Codec.NAME, si.getCodec().getName());
                        assertNotNull("the delegate codec is not recorded", si.getAttribute(DecoratedCodec.DELEGATE_ATTRIBUTE));
                        assertEquals(
                            MarkingCompoundDecorator.NAME + "," + PrefixingStoredFieldsDecorator.NAME,
                            si.getAttribute(DecoratedCodec.DECORATORS_ATTRIBUTE)
                        );

                        assertTrue("the segment is not a compound file", si.getUseCompoundFile());
                        assertEquals("the compound format of the plugin was not used", "applied", si.getAttribute(COMPOUND_ATTRIBUTE));
                    }
                }

                ++shards;
            }
        }

        assertEquals("no shards of the index were found", 1, shards);
    }

    /** Registered with the service loader, see the "META-INF/services" files of the test resources */
    public static class PrefixingStoredFieldsDecorator extends CodecDecorator {
        public static final String NAME = "ITPrefixingStoredFields";

        public PrefixingStoredFieldsDecorator() {
            super(NAME);
        }

        @Override
        public Optional<StoredFieldsFormat> storedFieldsFormat(Codec codec) {
            final StoredFieldsFormat delegate = codec.storedFieldsFormat();
            return Optional.of(new StoredFieldsFormat() {
                @Override
                public StoredFieldsReader fieldsReader(Directory directory, SegmentInfo si, FieldInfos fn, IOContext context)
                    throws IOException {
                    return new StrippingReader(delegate.fieldsReader(directory, si, fn, context));
                }

                @Override
                public StoredFieldsWriter fieldsWriter(Directory directory, SegmentInfo si, IOContext context) throws IOException {
                    return new PrefixingWriter(delegate.fieldsWriter(directory, si, context));
                }
            });
        }
    }

    /** Registered with the service loader, see the "META-INF/services" files of the test resources */
    public static class MarkingCompoundDecorator extends CodecDecorator {
        public static final String NAME = "ITMarkingCompound";

        public MarkingCompoundDecorator() {
            super(NAME);
        }

        @Override
        public Optional<CompoundFormat> compoundFormat(Codec codec, CompoundFormat delegate) {
            return Optional.of(new CompoundFormat() {
                @Override
                public CompoundDirectory getCompoundReader(Directory dir, SegmentInfo si) throws IOException {
                    assertEquals("applied", si.getAttribute(COMPOUND_ATTRIBUTE));
                    return delegate.getCompoundReader(dir, si);
                }

                @Override
                public void write(Directory dir, SegmentInfo si, IOContext context) throws IOException {
                    si.putAttribute(COMPOUND_ATTRIBUTE, "applied");
                    delegate.write(dir, si, context);
                }
            });
        }
    }

    private static class PrefixingWriter extends StoredFieldsWriter {
        private final StoredFieldsWriter delegate;

        private PrefixingWriter(StoredFieldsWriter delegate) {
            this.delegate = delegate;
        }

        @Override
        public void startDocument() throws IOException {
            delegate.startDocument();
        }

        @Override
        public void finishDocument() throws IOException {
            delegate.finishDocument();
        }

        @Override
        public void writeField(FieldInfo info, int value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, long value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, float value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, double value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, BytesRef value) throws IOException {
            delegate.writeField(info, value);
        }

        @Override
        public void writeField(FieldInfo info, String value) throws IOException {
            delegate.writeField(info, STORED_FIELDS_PREFIX + value);
        }

        @Override
        public void finish(int numDocs) throws IOException {
            delegate.finish(numDocs);
        }

        @Override
        public long ramBytesUsed() {
            return delegate.ramBytesUsed();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static class StrippingReader extends StoredFieldsReader {
        private final StoredFieldsReader delegate;

        private StrippingReader(StoredFieldsReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public void document(int docID, StoredFieldVisitor visitor) throws IOException {
            delegate.document(docID, new StoredFieldVisitor() {
                @Override
                public Status needsField(FieldInfo fieldInfo) throws IOException {
                    return visitor.needsField(fieldInfo);
                }

                @Override
                public void stringField(FieldInfo fieldInfo, String value) throws IOException {
                    visitor.stringField(fieldInfo, strip(value));
                }

                @Override
                public void binaryField(FieldInfo fieldInfo, byte[] value) throws IOException {
                    visitor.binaryField(fieldInfo, value);
                }

                @Override
                public void intField(FieldInfo fieldInfo, int value) throws IOException {
                    visitor.intField(fieldInfo, value);
                }

                @Override
                public void longField(FieldInfo fieldInfo, long value) throws IOException {
                    visitor.longField(fieldInfo, value);
                }

                @Override
                public void floatField(FieldInfo fieldInfo, float value) throws IOException {
                    visitor.floatField(fieldInfo, value);
                }

                @Override
                public void doubleField(FieldInfo fieldInfo, double value) throws IOException {
                    visitor.doubleField(fieldInfo, value);
                }

                private String strip(String value) {
                    return value.startsWith(STORED_FIELDS_PREFIX) ? value.substring(STORED_FIELDS_PREFIX.length()) : value;
                }
            });
        }

        @Override
        public StoredFieldsReader clone() {
            return new StrippingReader(delegate.clone());
        }

        @Override
        public void checkIntegrity() throws IOException {
            delegate.checkIntegrity();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
