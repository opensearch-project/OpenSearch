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
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.codecs.simpletext.SimpleTextCodec;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.AdditionalCodecs;
import org.opensearch.index.codec.CodecDecorator;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceConfig;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.codec.CompositeDelegatingPerFieldCodec;
import org.opensearch.index.codec.DecoratedCodec;
import org.opensearch.index.codec.DecoratedCodecTests;
import org.opensearch.index.codec.PerFieldCodecBuilder;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    public void testCreateEngineConfigFromFactoryPerFieldCodecBuilders() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new QuxEnginePlugin(new Lucene104PostingsFormat()));
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        final CodecService codecService = factory.newDefaultCodecService(indexSettings, null, logger);
        assertThat(codecService.codec("default"), is(instanceOf(CompositeDelegatingPerFieldCodec.class)));
    }

    public void testPerFieldCodecBuildersArePassedToCustomCodecServiceFactory() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        final PostingsFormat postingsFormat = new Lucene104PostingsFormat();
        final AtomicReference<CodecServiceConfig> captured = new AtomicReference<>();
        List<EnginePlugin> plugins = Arrays.asList(new QuxEnginePlugin(postingsFormat), new EnginePlugin() {
            @Override
            public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
                return Optional.of(config -> {
                    captured.set(config);
                    return new CodecService(
                        config.getMapperService(),
                        config.getIndexSettings(),
                        LogManager.getLogger(getClass()),
                        config.getAdditionalCodecs(),
                        config.getPerFieldCodecBuilders()
                    );
                });
            }
        });
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        final CodecService codecService = factory.newCodecServiceOrDefault(indexSettings, null, logger, null);
        assertThat(captured.get().getPerFieldCodecBuilders(), hasSize(1));
        assertThat(codecService.codec("default"), is(instanceOf(CompositeDelegatingPerFieldCodec.class)));
    }

    public void testCreateEngineConfigFromFactoryCodecDecorators() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new QuuxEnginePlugin(), new QuxEnginePlugin(new Lucene104PostingsFormat()));
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        final CodecService codecService = factory.newDefaultCodecService(indexSettings, null, logger);
        assertThat(codecService.codec("default"), is(instanceOf(DecoratedCodec.class)));
    }

    public void testCodecDecoratorsArePassedToCustomCodecServiceFactory() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        final AtomicReference<CodecServiceConfig> captured = new AtomicReference<>();
        List<EnginePlugin> plugins = Arrays.asList(new QuuxEnginePlugin(), new EnginePlugin() {
            @Override
            public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
                return Optional.of(config -> {
                    captured.set(config);
                    return new CodecService(
                        config.getMapperService(),
                        config.getIndexSettings(),
                        LogManager.getLogger(getClass()),
                        config.getAdditionalCodecs(),
                        config.getPerFieldCodecBuilders(),
                        config.getCodecDecorators()
                    );
                });
            }
        });
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        final CodecService codecService = factory.newCodecServiceOrDefault(indexSettings, null, logger, null);
        assertThat(captured.get().getCodecDecorators(), hasSize(1));
        assertThat(codecService.codec("default"), is(instanceOf(DecoratedCodec.class)));
    }

    public void testCodecServiceFactoryWhichDropsContributionsIsRejected() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new QuuxEnginePlugin(), new EnginePlugin() {
            @Override
            public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
                // the constructor which predates the codec contributions, the way the plugins use it today
                return Optional.of(
                    config -> new CodecService(
                        config.getMapperService(),
                        config.getIndexSettings(),
                        LogManager.getLogger(getClass()),
                        config.getAdditionalCodecs()
                    )
                );
            }
        });
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());

        // rejected along with the index creation, before any shard is started
        final IllegalStateException ex = expectThrows(IllegalStateException.class, () -> new EngineConfigFactory(plugins, indexSettings));
        assertThat(ex.getMessage(), containsString(QuuxEnginePlugin.class.getName()));
        assertThat(ex.getMessage(), containsString("CodecService(CodecServiceConfig)"));
    }

    public void testCodecServiceFactoryWhichDropsMappingDependentContributionsIsRejectedForShard() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new EnginePlugin() {
            @Override
            public Optional<CodecDecorator> getCodecDecorator(MapperService mapperService, IndexSettings indexSettings) {
                // only known once the mapping is
                return (mapperService != null) ? Optional.of(new DecoratedCodecTests.SideFileDecoratorA()) : Optional.empty();
            }
        }, new EnginePlugin() {
            @Override
            public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
                return Optional.of(
                    config -> new CodecService(
                        config.getMapperService(),
                        config.getIndexSettings(),
                        LogManager.getLogger(getClass()),
                        config.getAdditionalCodecs()
                    )
                );
            }
        });
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        final MapperService mapperService = mock(MapperService.class);
        when(mapperService.getIndexSettings()).thenReturn(indexSettings);
        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> factory.newCodecServiceOrDefault(indexSettings, mapperService, logger, null)
        );
        assertThat(ex.getMessage(), containsString("CodecService(CodecServiceConfig)"));
    }

    public void testCodecServiceFactoryWhichDropsNothingIsAccepted() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(new EnginePlugin() {
            @Override
            public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
                return Optional.of(
                    config -> new CodecService(
                        config.getMapperService(),
                        config.getIndexSettings(),
                        LogManager.getLogger(getClass()),
                        config.getAdditionalCodecs()
                    )
                );
            }
        });
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        final CodecService codecService = factory.newCodecServiceOrDefault(indexSettings, null, logger, null);
        assertFalse(codecService.composesPluginCodecs());
    }

    public void testCodecServiceFactoryUsingConfigComposesContributions() {
        IndexMetadata meta = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        List<EnginePlugin> plugins = Arrays.asList(
            new QuuxEnginePlugin(),
            new QuxEnginePlugin(new Lucene104PostingsFormat()),
            new EnginePlugin() {
                @Override
                public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
                    return Optional.of(CodecService::new);
                }
            }
        );
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", meta.getSettings());
        EngineConfigFactory factory = new EngineConfigFactory(plugins, indexSettings);

        final CodecService codecService = factory.newCodecServiceOrDefault(indexSettings, null, logger, null);
        assertTrue(codecService.composesPluginCodecs());
        assertThat(codecService.codec("default"), is(instanceOf(DecoratedCodec.class)));
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

    /**
     * A shard can become a primary without a new engine, and therefore without a new config, so a single
     * config must re-consult the plugins on every read rather than caching the answer from build time.
     */
    public void testPrimaryOperationPolicyReconsultedOnEachReadOfOneConfig() {
        IndexSettings indexSettings = newIndexSettings();
        AtomicReference<PrimaryOperationPolicy> pluginPolicy = new AtomicReference<>();
        EnginePlugin plugin = new EnginePlugin() {
            @Override
            public Optional<PrimaryOperationPolicy> getPrimaryOperationPolicy(IndexSettings settings) {
                return Optional.ofNullable(pluginPolicy.get());
            }
        };
        EngineConfigFactory factory = new EngineConfigFactory(Collections.singletonList(plugin), indexSettings);
        EngineConfig config = newEngineConfig(factory, indexSettings);
        assertSame(DefaultPrimaryOperationPolicy.INSTANCE, config.getPrimaryOperationPolicy());

        // the plugin starts overriding the policy, e.g. because a setting it keys off was updated
        pluginPolicy.set(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE);
        assertSame(FakePreAssignedSeqNoPrimaryOperationPolicy.INSTANCE, config.getPrimaryOperationPolicy());

        // and a config copied from it carries the resolver rather than a stale resolved policy
        pluginPolicy.set(null);
        assertSame(DefaultPrimaryOperationPolicy.INSTANCE, config.toBuilder().build().getPrimaryOperationPolicy());
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

    private static class QuuxEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<CodecDecorator> getCodecDecorator(MapperService mapperService, IndexSettings indexSettings) {
            return Optional.of(new DecoratedCodecTests.SideFileDecoratorA());
        }
    }

    private static class QuxEnginePlugin extends Plugin implements EnginePlugin {
        private final PerFieldPostingsFormat postingsFormat;

        QuxEnginePlugin(final PostingsFormat postingsFormat) {
            this.postingsFormat = new PerFieldPostingsFormat() {
                @Override
                public PostingsFormat getPostingsFormatForField(String field) {
                    return "claimed".equals(field) ? postingsFormat : null;
                }
            };
        }

        @Override
        public Optional<PerFieldCodecBuilder> getPerFieldCodecBuilder(MapperService mapperService, IndexSettings indexSettings) {
            return Optional.of(new PerFieldCodecBuilder() {
                @Override
                public Optional<PerFieldPostingsFormat> postingsFormat() {
                    return Optional.of(postingsFormat);
                }
            });
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
