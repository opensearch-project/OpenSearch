/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.datacube.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.codec.composite.composite99.Composite99Codec;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.indices.IndicesModule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collections;

import static org.opensearch.common.util.FeatureFlags.STAR_TREE_INDEX;

/**
 * Star tree doc values Lucene tests
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public class StarTreeDocValuesFormatTests extends BaseDocValuesFormatTestCase {
    MapperService mapperService = null;

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
            createMapperService(getExpandedMapping("status", "size"));
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
        doc.add(new SortedNumericDocValuesField("field", 1));
        iw.addDocument(doc);
        doc.add(new SortedNumericDocValuesField("sndv", 1));
        doc.add(new SortedNumericDocValuesField("dv", 1));
        doc.add(new SortedNumericDocValuesField("field", 1));
        iw.addDocument(doc);
        iw.forceMerge(1);
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedNumericDocValuesField("dv", 2));
        doc.add(new SortedNumericDocValuesField("field", 2));
        iw.addDocument(doc);
        doc.add(new SortedNumericDocValuesField("sndv", 2));
        doc.add(new SortedNumericDocValuesField("dv", 2));
        doc.add(new SortedNumericDocValuesField("field", 2));
        iw.addDocument(doc);
        iw.forceMerge(1);
        iw.close();

        // TODO : validate star tree structures that got created
        directory.close();
    }

    private XContentBuilder getExpandedMapping(String dim, String metric) throws IOException {
        return topMapping(b -> {
            b.startObject("composite");
            b.startObject("startree");
            b.field("type", "star_tree");
            b.startObject("config");
            b.field("max_leaf_docs", 100);
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
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
            .putMapping(builder.toString())
            .build();
        IndicesModule indicesModule = new IndicesModule(Collections.emptyList());
        mapperService = MapperTestUtils.newMapperServiceWithHelperAnalyzer(
            new NamedXContentRegistry(ClusterModule.getNamedXWriteables()),
            createTempDir(),
            Settings.EMPTY,
            indicesModule,
            "test"
        );
        mapperService.merge(indexMetadata, MapperService.MergeReason.INDEX_TEMPLATE);
    }
}
