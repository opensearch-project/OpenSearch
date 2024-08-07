/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite.datacube.startree;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

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
import org.opensearch.common.Rounding;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.codec.composite.Composite99Codec;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.StarTreeMapper;
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

    private StarTreeMapper.StarTreeFieldType getStarTreeFieldType() {
        List<MetricStat> m1 = new ArrayList<>();
        m1.add(MetricStat.MAX);
        Metric metric = new Metric("sndv", m1);
        List<Rounding.DateTimeUnit> d1CalendarIntervals = new ArrayList<>();
        d1CalendarIntervals.add(Rounding.DateTimeUnit.HOUR_OF_DAY);
        StarTreeField starTreeField = getStarTreeField(d1CalendarIntervals, metric);

        return new StarTreeMapper.StarTreeFieldType("star_tree", starTreeField);
    }

    private StarTreeField getStarTreeField(List<Rounding.DateTimeUnit> d1CalendarIntervals, Metric metric1) {
        DateDimension d1 = new DateDimension("field", d1CalendarIntervals);
        NumericDimension d2 = new NumericDimension("dv");

        List<Metric> metrics = List.of(metric1);
        List<Dimension> dims = List.of(d1, d2);
        StarTreeFieldConfiguration config = new StarTreeFieldConfiguration(100, Collections.emptySet(), buildMode);

        return new StarTreeField("starTree", dims, metrics, config);
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

    private XContentBuilder getExpandedMapping() throws IOException {
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
