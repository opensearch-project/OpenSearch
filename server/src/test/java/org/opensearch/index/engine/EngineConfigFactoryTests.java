/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.AdditionalCodecs;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.translog.InternalTranslogFactory;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogDeletionPolicyFactory;
import org.opensearch.index.translog.TranslogReader;
import org.opensearch.index.translog.TranslogWriter;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;

public class EngineConfigFactoryTests extends OpenSearchTestCase {
    public void testCreateEngineConfigFromFactory() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Collections.singletonList(new FooEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        EngineConfig config = factory.newEngineConfig(
            null,
            null,
            indexSettings,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            TimeValue.timeValueMinutes(5),
            null,
            null,
            null,
            null,
            null,
            () -> new RetentionLeases(0, 0, Collections.emptyList()),
            null,
            null,
            false,
            () -> Boolean.TRUE,
            new InternalTranslogFactory(),
            null,
            null,
            QueryBitSetProducer::new,
            null,
            null,
            null,
            null,
            null,
            null
        );

        assertNotNull(config.getCodec());
        assertNotNull(config.getCustomTranslogDeletionPolicyFactory());
        assertTrue(config.getCustomTranslogDeletionPolicyFactory().create(indexSettings, null) instanceof CustomTranslogDeletionPolicy);
    }

    public void testCreateEngineConfigFromFactoryMultipleCodecServiceIllegalStateException() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new FooEnginePlugin(), new BarEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());

        expectThrows(IllegalStateException.class, () -> new EngineConfigFactory(plugins, indexSettings));
    }

    public void testCreateEngineConfigFromFactoryMultipleCodecServiceAndFactoryIllegalStateException() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new FooEnginePlugin(), new BakEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());

        expectThrows(IllegalStateException.class, () -> new EngineConfigFactory(plugins, indexSettings));
    }

    public void testCreateEngineConfigFromFactoryMultipleCustomTranslogDeletionPolicyFactoryIllegalStateException() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new FooEnginePlugin(), new BazEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());

        expectThrows(IllegalStateException.class, () -> new EngineConfigFactory(plugins, indexSettings));
    }

    public void testCreateEngineConfigFromFactoryAdditionalCodecs() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new BazEnginePlugin(Map.of("test", new SimpleTextCodec())));
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        final CodecService codecService = factory.newDefaultCodecService(indexSettings, null, logger);
        assertThat(codecService.codec("test"), is(instanceOf(SimpleTextCodec.class)));
    }

    public void testCreateEngineConfigFromFactoryAdditionalCodecsConflict() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new BazEnginePlugin(Map.of("zlib", new SimpleTextCodec())));
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        assertThrows(IllegalStateException.class, () -> factory.newDefaultCodecService(indexSettings, null, logger));
    }

    public void testCreateCodecServiceFromFactory() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new BakEnginePlugin());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());

        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);
        EngineConfig config = factory.newEngineConfig(
            null,
            null,
            indexSettings,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            TimeValue.timeValueMinutes(5),
            null,
            null,
            null,
            null,
            null,
            () -> new RetentionLeases(0, 0, Collections.emptyList()),
            null,
            null,
            false,
            () -> Boolean.TRUE,
            new InternalTranslogFactory(),
            null,
            null,
            QueryBitSetProducer::new,
            null,
            null,
            null,
            null,
            null,
            null
        );
        assertNotNull(config.getCodec());
    }

    public void testGetEngineFactory() {
        final EngineFactory engineFactory = config -> null;
        EnginePlugin enginePluginThatImplementsGetEngineFactory = new EnginePlugin() {
            @Override
            public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
                return Optional.of(engineFactory);
            }
        };
        assertEquals(engineFactory, enginePluginThatImplementsGetEngineFactory.getEngineFactory(null).orElse(null));

        EnginePlugin enginePluginThatDoesNotImplementsGetEngineFactory = new EnginePlugin() {
        };
        assertFalse(enginePluginThatDoesNotImplementsGetEngineFactory.getEngineFactory(null).isPresent());
    }

    public void testGetPrimaryOperationPolicyIsEmptyByDefault() {
        EnginePlugin plugin = new EnginePlugin() {
        };
        assertFalse(plugin.getPrimaryOperationPolicy(null).isPresent());
    }

    public void testPrimaryOperationPolicyDefaultsWhenNoPluginSuppliesOne() {
        IndexSettings indexSettings = newIndexSettings();
        EngineConfigFactory factory = new EngineConfigFactory(Collections.singletonList(new FooEnginePlugin()), indexSettings);
        EngineConfig config = newEngineConfig(factory, indexSettings);
        assertThat(config.getPrimaryOperationPolicy(), is(DefaultPrimaryOperationPolicy.INSTANCE));
        assertSame(DefaultPrimaryOperationPolicy.INSTANCE, config.getPrimaryOperationPolicy());
    }

    public void testPrimaryOperationPolicyFromPlugin() {
        IndexSettings indexSettings = newIndexSettings();
        EngineConfigFactory factory = new EngineConfigFactory(
            Collections.singletonList(new PrimaryOperationPolicyEnginePlugin(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE)),
            indexSettings
        );
        EngineConfig config = newEngineConfig(factory, indexSettings);
        assertThat(config.getPrimaryOperationPolicy(), is(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE));
        assertNotSame(DefaultPrimaryOperationPolicy.INSTANCE, config.getPrimaryOperationPolicy());
    }

    public void testMultiplePrimaryOperationPoliciesIllegalStateException() {
        IndexSettings indexSettings = newIndexSettings();
        List<EnginePlugin> plugins = Arrays.asList(
            new PrimaryOperationPolicyEnginePlugin(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE),
            new PrimaryOperationPolicyEnginePlugin(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE)
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> new EngineConfigFactory(plugins, indexSettings));
        assertTrue(e.getMessage(), e.getMessage().contains("PrimaryOperationPolicy is already overridden"));
    }

    public void testMultiplePrimaryOperationPoliciesIllegalStateExceptionOnEngineConfig() {
        IndexSettings indexSettings = newIndexSettings();
        AtomicReference<PrimaryOperationPolicy> latePluginPolicy = new AtomicReference<>();
        List<EnginePlugin> plugins = Arrays.asList(
            new PrimaryOperationPolicyEnginePlugin(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE),
            new EnginePlugin() {
                @Override
                public Optional<PrimaryOperationPolicy> getPrimaryOperationPolicy(IndexSettings settings) {
                    return Optional.ofNullable(latePluginPolicy.get());
                }
            }
        );
        // only one plugin overrides the policy at construction, so the conflict cannot be detected up front
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);
        assertSame(
            FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE,
            newEngineConfig(factory, indexSettings).getPrimaryOperationPolicy()
        );

        // the second plugin starts overriding it too, so the next engine build must reject the conflict
        latePluginPolicy.set(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> newEngineConfig(factory, indexSettings));
        assertTrue(e.getMessage(), e.getMessage().contains("PrimaryOperationPolicy is already overridden"));
    }

    public void testPrimaryOperationPolicyReconsultedOnEachEngineConfig() {
        IndexSettings indexSettings = newIndexSettings();
        AtomicReference<PrimaryOperationPolicy> pluginPolicy = new AtomicReference<>(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE);
        EnginePlugin plugin = new EnginePlugin() {
            @Override
            public Optional<PrimaryOperationPolicy> getPrimaryOperationPolicy(IndexSettings settings) {
                return Optional.ofNullable(pluginPolicy.get());
            }
        };
        EngineConfigFactory factory = new EngineConfigFactory(Collections.singletonList(plugin), indexSettings);

        assertSame(
            FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE,
            newEngineConfig(factory, indexSettings).getPrimaryOperationPolicy()
        );

        // the plugin changes its answer between engine builds (e.g. a replication role flip); the next
        // config must reflect it without a new factory
        pluginPolicy.set(null);
        assertSame(DefaultPrimaryOperationPolicy.INSTANCE, newEngineConfig(factory, indexSettings).getPrimaryOperationPolicy());
    }

    private static IndexSettings newIndexSettings() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        return IndexSettingsModule.newIndexSettings("test", meta.getSettings());
    }

    private static EngineConfig newEngineConfig(EngineConfigFactory factory, IndexSettings indexSettings) {
        return factory.newEngineConfig(
            null,
            null,
            indexSettings,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            TimeValue.timeValueMinutes(5),
            null,
            null,
            null,
            null,
            null,
            () -> new RetentionLeases(0, 0, Collections.emptyList()),
            null,
            null,
            false,
            () -> Boolean.TRUE,
            new InternalTranslogFactory(),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }

    private static class PrimaryOperationPolicyEnginePlugin extends Plugin implements EnginePlugin {
        private final PrimaryOperationPolicy provider;

        PrimaryOperationPolicyEnginePlugin(PrimaryOperationPolicy provider) {
            this.provider = provider;
        }

        @Override
        public Optional<PrimaryOperationPolicy> getPrimaryOperationPolicy(IndexSettings indexSettings) {
            return Optional.of(provider);
        }
    }

    private static class FooEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            return Optional.empty();
        }

        @Override
        public Optional<CodecService> getCustomCodecService(IndexSettings indexSettings) {
            return Optional.of(new CodecService(null, indexSettings, LogManager.getLogger(getClass()), List.of()));
        }

        @Override
        public Optional<TranslogDeletionPolicyFactory> getCustomTranslogDeletionPolicyFactory() {
            return Optional.of(CustomTranslogDeletionPolicy::new);
        }
    }

    private static class BarEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            return Optional.empty();
        }

        @Override
        public Optional<CodecService> getCustomCodecService(IndexSettings indexSettings) {
            return Optional.of(new CodecService(null, indexSettings, LogManager.getLogger(getClass()), List.of()));
        }
    }

    private static class BakEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            return Optional.empty();
        }

        @Override
        public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
            return Optional.of(
                config -> new CodecService(
                    config.getMapperService(),
                    config.getIndexSettings(),
                    LogManager.getLogger(getClass()),
                    List.of()
                )
            );
        }
    }

    private static class BazEnginePlugin extends Plugin implements EnginePlugin {
        private final Map<String, Codec> additionalCodecs;

        BazEnginePlugin() {
            this(Map.of());
        }

        BazEnginePlugin(final Map<String, Codec> additionalCodecs) {
            this.additionalCodecs = additionalCodecs;
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(final IndexSettings indexSettings) {
            return Optional.empty();
        }

        @Override
        public Optional<TranslogDeletionPolicyFactory> getCustomTranslogDeletionPolicyFactory() {
            return Optional.of(CustomTranslogDeletionPolicy::new);
        }

        @Override
        public Optional<AdditionalCodecs> getAdditionalCodecs(IndexSettings indexSettings) {
            return Optional.of(new AdditionalCodecs() {
                @Override
                public Map<String, Codec> getCodecs(
                    MapperService mapperService,
                    IndexSettings indexSettings,
                    Supplier<Codec> defaultCodec
                ) {
                    return additionalCodecs;
                }
            });
        }
    }

    private static class CustomTranslogDeletionPolicy extends TranslogDeletionPolicy {
        public CustomTranslogDeletionPolicy(IndexSettings indexSettings, Supplier<RetentionLeases> retentionLeasesSupplier) {
            super();
        }

        @Override
        public void setRetentionSizeInBytes(long bytes) {

        }

        @Override
        public void setRetentionAgeInMillis(long ageInMillis) {

        }

        @Override
        protected void setRetentionTotalFiles(int retentionTotalFiles) {

        }

        @Override
        public long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter writer) throws IOException {
            return 0;
        }
    }
}
