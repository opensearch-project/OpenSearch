/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite99.datacube.startree;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.codec.composite.CompositeIndexReader;
import org.opensearch.index.codec.composite.composite99.Composite99Codec;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeDocument;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeTestUtils;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.indices.IndicesModule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.opensearch.common.util.FeatureFlags.STAR_TREE_INDEX;
import static org.opensearch.index.compositeindex.datacube.startree.StarTreeTestUtils.assertStarTreeDocuments;

/**
 * Star tree doc values Lucene tests
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class StarTreeDocValuesFormatTests extends BaseDocValuesFormatTestCase {
    MapperService mapperService = null;
    StarTreeFieldConfiguration.StarTreeBuildMode buildMode;

    public StarTreeDocValuesFormatTests(StarTreeFieldConfiguration.StarTreeBuildMode buildMode) {
        this.buildMode = buildMode;
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        List<Object[]> parameters = new ArrayList<>();
        parameters.add(new Object[] { StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP });
        parameters.add(new Object[] { StarTreeFieldConfiguration.StarTreeBuildMode.OFF_HEAP });
        return parameters;
    }

    @BeforeClass
    public static void createMapper() throws Exception {
        FeatureFlags.initializeFeatureFlags(Settings.builder().put(STAR_TREE_INDEX, "true").build());
    }

    @AfterClass
    public static void clearMapper() {
        FeatureFlags.initializeFeatureFlags(Settings.EMPTY);
    }

    @After
    public void teardown() throws IOException {
        mapperService.close();
    }

    @Override
    protected Codec getCodec() {
        final Logger testLogger = LogManager.getLogger(StarTreeDocValuesFormatTests.class);

        try {
            createMapperService(getExpandedMapping());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Codec codec = new Composite99Codec(Lucene99Codec.Mode.BEST_SPEED, mapperService, testLogger);
        return codec;
    }

    public void testStarTreeDocValues() throws IOException {
        Directory directory = newDirectory();
        IndexWriterConfig conf = newIndexWriterConfig(null);
        conf.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), directory, conf);
        Document doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedNumericDocValuesField("dv", 1));
        doc.add(new SortedNumericDocValuesField("field", -1));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedNumericDocValuesField("dv", 1));
        doc.add(new SortedNumericDocValuesField("field", -1));
        iw.addDocument(doc);
        doc = new Document();
        iw.forceMerge(1);
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedNumericDocValuesField("dv", 2));
        doc.add(new SortedNumericDocValuesField("field", -2));
        iw.addDocument(doc);
        doc = new Document();
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedNumericDocValuesField("dv", 2));
        doc.add(new SortedNumericDocValuesField("field", -2));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        DirectoryReader ir = maybeWrapWithMergingReader(DirectoryReader.open(directory));
        TestUtil.checkReader(ir);
        assertEquals(1, ir.leaves().size());

        // Segment documents
        /**
         * sndv dv field
         * [1,  1,   -1]
         * [1,  1,   -1]
         * [2,  2,   -2]
         * [2,  2,   -2]
         */
        // Star tree docuements
        /**
         * sndv dv | [ sum, value_count, min, max[field]] , [ sum, value_count, min, max[sndv]], doc_count
         * [1, 1] | [-2.0, 2.0, -1.0, -1.0, 2.0, 2.0, 1.0, 1.0, 2.0]
         * [2, 2] | [-4.0, 2.0, -2.0, -2.0, 4.0, 2.0, 2.0, 2.0, 2.0]
         * [null, 1] | [-2.0, 2.0, -1.0, -1.0, 2.0, 2.0, 1.0, 1.0, 2.0]
         * [null, 2] | [-4.0, 2.0, -2.0, -2.0, 4.0, 2.0, 2.0, 2.0, 2.0]
         */
        StarTreeDocument[] expectedStarTreeDocuments = new StarTreeDocument[4];
        expectedStarTreeDocuments[0] = new StarTreeDocument(
            new Long[] { 1L, 1L },
            new Double[] { -2.0, 2.0, -1.0, -1.0, 2.0, 2.0, 1.0, 1.0, 2.0 }
        );
        expectedStarTreeDocuments[1] = new StarTreeDocument(
            new Long[] { 2L, 2L },
            new Double[] { -4.0, 2.0, -2.0, -2.0, 4.0, 2.0, 2.0, 2.0, 2.0 }
        );
        expectedStarTreeDocuments[2] = new StarTreeDocument(
            new Long[] { null, 1L },
            new Double[] { -2.0, 2.0, -1.0, -1.0, 2.0, 2.0, 1.0, 1.0, 2.0 }
        );
        expectedStarTreeDocuments[3] = new StarTreeDocument(
            new Long[] { null, 2L },
            new Double[] { -4.0, 2.0, -2.0, -2.0, 4.0, 2.0, 2.0, 2.0, 2.0 }
        );

        for (LeafReaderContext context : ir.leaves()) {
            SegmentReader reader = Lucene.segmentReader(context.reader());
            CompositeIndexReader starTreeDocValuesReader = (CompositeIndexReader) reader.getDocValuesReader();
            List<CompositeIndexFieldInfo> compositeIndexFields = starTreeDocValuesReader.getCompositeIndexFields();

            for (CompositeIndexFieldInfo compositeIndexFieldInfo : compositeIndexFields) {
                StarTreeValues starTreeValues = (StarTreeValues) starTreeDocValuesReader.getCompositeIndexValues(compositeIndexFieldInfo);
                StarTreeDocument[] starTreeDocuments = StarTreeTestUtils.getSegmentsStarTreeDocuments(
                    List.of(starTreeValues),
                    List.of(
                        StarTreeNumericType.DOUBLE,
                        StarTreeNumericType.LONG,
                        StarTreeNumericType.DOUBLE,
                        StarTreeNumericType.DOUBLE,
                        StarTreeNumericType.DOUBLE,
                        StarTreeNumericType.LONG,
                        StarTreeNumericType.DOUBLE,
                        StarTreeNumericType.DOUBLE,
                        StarTreeNumericType.LONG
                    ),
                    reader.maxDoc()
                );
                assertStarTreeDocuments(starTreeDocuments, expectedStarTreeDocuments);
            }
        }
        ir.close();
        directory.close();
    }

    private XContentBuilder getExpandedMapping() throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 1);
            b.startArray("ordered_dimensions");
            b.startObject();
            b.field("name", "sndv");
            b.endObject();
            b.startObject();
            b.field("name", "dv");
            b.endObject();
            b.endArray();
            b.startArray("metrics");
            b.startObject();
            b.field("name", "field");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("avg");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.startObject();
            b.field("name", "sndv");
            b.startArray("stats");
            b.value("sum");
            b.value("value_count");
            b.value("avg");
            b.value("min");
            b.value("max");
            b.endArray();
            b.endObject();
            b.endArray();
            b.endObject();
            b.endObject();
            b.endObject();
            b.startObject("properties");
            b.startObject("sndv");
            b.field("type", "integer");
            b.endObject();
            b.startObject("dv");
            b.field("type", "integer");
            b.endObject();
            b.startObject("field");
            b.field("type", "integer");
            b.endObject();
            b.endObject();
        });
    }

    private XContentBuilder topMapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        buildFields.accept(builder);
        return builder.endObject().endObject();
    }

    private void createMapperService(XContentBuilder builder) throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(StarTreeIndexSettings.IS_COMPOSITE_INDEX_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(512, ByteSizeUnit.MB))
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).putMapping(builder.toString()).build();
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        mapperService = MapperTestUtils.newMapperServiceWithHelperAnalyzer(
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(),
            settings,
            indicesModule,
            "test"
        );
        mapperService.merge(indexMetadata, MapperService.MergeReason.INDEX_TEMPLATE);
    }
}
