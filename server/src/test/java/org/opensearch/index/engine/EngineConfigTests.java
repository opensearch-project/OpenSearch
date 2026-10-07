/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.codecs.Codec;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class EngineConfigTests extends OpenSearchTestCase {

    private IndexSettings defaultIndexSettings;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        final IndexMetadata defaultIndexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        defaultIndexSettings = IndexSettingsModule.newIndexSettings("test", defaultIndexMetadata.getSettings());
    }

    public void testEngineConfig_DefaultValueFoUseCompoundFile() {
        EngineConfig config = new EngineConfig.Builder().indexSettings(defaultIndexSettings)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .build();
        assertTrue(config.useCompoundFile());
    }

    public void testEngineConfig_DefaultValueForReadOnlyEngine() {
        EngineConfig config = new EngineConfig.Builder().indexSettings(defaultIndexSettings)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .build();
        assertFalse(config.isReadOnlyReplica());
    }

    public void testEngineConfig_ReadOnlyEngineWithSegRepDisabled() {
        expectThrows(IllegalArgumentException.class, () -> createReadOnlyEngine(defaultIndexSettings));
    }

    public void testEngineConfig_ReadOnlyEngineWithSegRepEnabled() {
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder()
                .put(defaultIndexSettings.getSettings())
                .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
                .build()
        );
        EngineConfig engineConfig = createReadOnlyEngine(indexSettings);
        assertTrue(engineConfig.isReadOnlyReplica());
    }

    public void testEngineConfig_ToBuilderRoundTripPreservesFields() {
        DataFormatRegistry dataFormatRegistry = mock(DataFormatRegistry.class);
        MapperService mapperService = mock(MapperService.class);
        CommitterFactory committerFactory = mock(CommitterFactory.class);
        Map<String, FormatChecksumStrategy> checksumStrategies = Map.of("test-format", mock(FormatChecksumStrategy.class));

        EngineConfig config = new EngineConfig.Builder().indexSettings(defaultIndexSettings)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .dataFormatRegistry(dataFormatRegistry)
            .mapperService(mapperService)
            .committerFactory(committerFactory)
            .checksumStrategies(checksumStrategies)
            .build();

        EngineConfig copy = config.toBuilder().build();
        assertSame(dataFormatRegistry, copy.getDataFormatRegistry());
        assertSame(mapperService, copy.getMapperService());
        assertSame(committerFactory, copy.getCommitterFactory());
        assertSame(checksumStrategies, copy.getChecksumStrategies());
    }

    public void testInvalidCodecMessageListsAllAcceptedNames() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EngineConfig.INDEX_CODEC_SETTING.get(Settings.builder().put("index.codec", "not_a_codec").build())
        );
        String message = e.getMessage();
        assertTrue(message, message.contains("lucene_default"));
        assertTrue(message, message.contains("best_compression"));
        for (String codec : Codec.availableCodecs()) {
            assertTrue(message, message.contains(codec));
        }
    }

    public void testValidCodecNamesAreAccepted() {
        for (String codec : List.of("default", "lz4", "best_compression", "zlib", "lucene_default")) {
            assertEquals(codec, EngineConfig.INDEX_CODEC_SETTING.get(Settings.builder().put("index.codec", codec).build()));
        }
    }

    private EngineConfig createReadOnlyEngine(IndexSettings indexSettings) {
        return new EngineConfig.Builder().indexSettings(indexSettings)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .readOnlyReplica(true)
            .build();
    }
}
