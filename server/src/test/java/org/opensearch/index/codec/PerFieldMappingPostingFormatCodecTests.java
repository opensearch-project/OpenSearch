/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.fuzzy.FuzzyFilterPostingsFormat;
import org.opensearch.index.mapper.IdFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PerFieldMappingPostingFormatCodecTests extends OpenSearchTestCase {

    private MapperService mapperService;
    private Logger logger;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mapperService = mock(MapperService.class);
        logger = mock(Logger.class);
    }

    public void testFuzzySetEnabledSetting() {
        Settings settings = Settings.builder().put("index.optimize_doc_id_lookup.fuzzy_set.enabled", true).build();

        boolean enabled = IndexSettings.INDEX_DOC_ID_FUZZY_SET_ENABLED_SETTING.get(settings);

        assertTrue("Fuzzy set should be enabled", enabled);
    }

    public void testFuzzySetDisabled() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.doc_id_fuzzy_set.enabled", false)
            .build();

        IndexMetadata metadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();

        IndexSettings indexSettings = new IndexSettings(metadata, settings);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);

        boolean isEnabled = mapperService.getIndexSettings().isEnableFuzzySetForDocId();
        assertFalse("Fuzzy set should be disabled", isEnabled);

        PerFieldMappingPostingFormatCodec codec = new PerFieldMappingPostingFormatCodec(
            Lucene103Codec.Mode.BEST_SPEED,
            mapperService,
            logger
        );

        PostingsFormat format = codec.getPostingsFormatForField(IdFieldMapper.NAME);
        assertFalse("Should not be FuzzyFilterPostingsFormat when disabled", format instanceof FuzzyFilterPostingsFormat);
    }

    public void testFuzzyFilterPostingsFormatIsUsedForDocId() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.optimize_doc_id_lookup.fuzzy_set.enabled", true)
            .put("index.optimize_doc_id_lookup.fuzzy_set.false_positive_probability", 0.05)
            .build();

        IndexMetadata metadata = IndexMetadata.builder("test").settings(settings).numberOfShards(1).numberOfReplicas(0).build();

        IndexSettings indexSettings = new IndexSettings(metadata, settings);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);

        MappedFieldType mockFieldType = mock(MappedFieldType.class);
        when(mapperService.fieldType(IdFieldMapper.NAME)).thenReturn(mockFieldType);

        PerFieldMappingPostingFormatCodec codec = new PerFieldMappingPostingFormatCodec(
            Lucene103Codec.Mode.BEST_SPEED,
            mapperService,
            logger
        );

        PostingsFormat format = codec.getPostingsFormatForField(IdFieldMapper.NAME);

        assertTrue(
            "FuzzyFilterPostingsFormat should be used for _id when fuzzy set is enabled",
            format instanceof FuzzyFilterPostingsFormat
        );
    }
}
