/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecSettings;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

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

    private EngineConfig createReadOnlyEngine(IndexSettings indexSettings) {
        return new EngineConfig.Builder().indexSettings(indexSettings)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .readOnlyReplica(true)
            .build();
    }

    public void testExperimentalCodecs() throws Exception {
        class ExperimentalCodec extends FilterCodec implements CodecSettings {

            final boolean isExperimental;

            protected ExperimentalCodec(String name, Codec delegate, boolean isExperimental) {
                super(name, delegate);
                this.isExperimental = isExperimental;
            }

            @Override
            public boolean supports(Setting<?> setting) {
                return false;
            }

            @Override
            public boolean experimental() {
                return isExperimental;
            }
        }
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EngineConfig.isExperimentalCodec(new ExperimentalCodec("experimentalcodec", new Lucene99Codec(), true))
        );
        assertTrue(e.getMessage().startsWith("experimental codecs are not enabled."));
        assertFalse(EngineConfig.isExperimentalCodec(new ExperimentalCodec("nonexperimentalcodec", new Lucene99Codec(), false)));
    }

}
