/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite912.datacube.startree;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.Version;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MapperTestUtils;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.MapperService;
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
 * Abstract star tree doc values Lucene tests
 */
@LuceneTestCase.SuppressSysoutChecks(bugUrl = "we log a lot on purpose")
public abstract class AbstractStarTreeDVFormatTests extends BaseDocValuesFormatTestCase {
    MapperService mapperService = null;
    StarTreeFieldConfiguration.StarTreeBuildMode buildMode;

    public AbstractStarTreeDVFormatTests(StarTreeFieldConfiguration.StarTreeBuildMode buildMode) {
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
        Codec codec = new Composite912Codec(Lucene912Codec.Mode.BEST_SPEED, mapperService, testLogger);
        return codec;
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

    abstract XContentBuilder getExpandedMapping() throws IOException;

    XContentBuilder topMapping(CheckedConsumer<XContentBuilder, IOException> buildFields) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject("_doc");
        buildFields.accept(builder);
        return builder.endObject().endObject();
    }

}
