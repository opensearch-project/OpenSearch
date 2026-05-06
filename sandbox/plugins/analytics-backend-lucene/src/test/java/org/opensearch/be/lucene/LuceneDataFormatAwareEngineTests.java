/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.Version;
import org.opensearch.be.lucene.index.LuceneCommitterFactory;
import org.opensearch.be.lucene.index.LuceneDocumentInput;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.dataformat.AbstractDataFormatAwareEngineTestCase;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TranslogConfig;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.IndexSettingsModule;

import java.nio.file.Path;
import java.util.List;

import static org.opensearch.index.engine.EngineTestCase.tombstoneDocSupplier;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Runs the {@link AbstractDataFormatAwareEngineTestCase} suite with the real
 * Lucene {@link LucenePlugin} engine, validating that the DFAE orchestration
 * layer (indexing, refresh, flush, concurrency, catalog snapshots) works
 * correctly with Lucene's inverted index writer and segment management.
 *
 * <p>Overrides {@link #buildEngineConfig} to provide the {@link CodecService}
 * and {@link StandardAnalyzer} that the {@link LuceneCommitterFactory} requires
 * for creating the shared {@code IndexWriter}.
 */
public class LuceneDataFormatAwareEngineTests extends AbstractDataFormatAwareEngineTestCase {

    @Override
    protected DataFormatPlugin createDataFormatPlugin() {
        return new LucenePlugin();
    }

    @Override
    protected SearchBackEndPlugin<?> createSearchBackEndPlugin() {
        return new LucenePlugin();
    }

    @Override
    protected String dataFormatName() {
        return LuceneDataFormat.LUCENE_FORMAT_NAME;
    }

    @Override
    protected CommitterFactory createCommitterFactory(Store store) {
        return new LuceneCommitterFactory();
    }

    @Override
    protected DocumentInput<?> createDocumentInput() {
        return new LuceneDocumentInput(new LuceneFieldFactoryRegistry());
    }

    @Override
    @SuppressWarnings("unchecked")
    protected EngineConfig buildEngineConfig(
        Store store,
        Path translogPath,
        List<ReferenceManager.RefreshListener> externalListeners,
        List<ReferenceManager.RefreshListener> internalListeners
    ) {
        Settings.Builder settingsBuilder = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), dataFormatName())
            .put(additionalIndexSettings());
        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", settingsBuilder.build());

        TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            translogPath,
            indexSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            "",
            false
        );

        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(createDataFormatPlugin()));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(createSearchBackEndPlugin()));
        DataFormatRegistry registry = new DataFormatRegistry(pluginsService);

        CodecService codecService = new CodecService(null, indexSettings, LogManager.getLogger(getClass()), List.of());

        MapperService mapperService = mock(MapperService.class);
        MappedFieldType versionFieldType = mock(MappedFieldType.class);
        when(versionFieldType.typeName()).thenReturn(VersionFieldMapper.CONTENT_TYPE);
        when(versionFieldType.name()).thenReturn(VersionFieldMapper.NAME);
        MappedFieldType seqNoFieldType = mock(MappedFieldType.class);
        when(seqNoFieldType.typeName()).thenReturn(SeqNoFieldMapper.CONTENT_TYPE);
        when(seqNoFieldType.name()).thenReturn(SeqNoFieldMapper.NAME);
        when(mapperService.fieldType(VersionFieldMapper.NAME)).thenReturn(versionFieldType);
        when(mapperService.fieldType(SeqNoFieldMapper.NAME)).thenReturn(seqNoFieldType);

        return new EngineConfig.Builder().shardId(shardId)
            .threadPool(threadPool)
            .indexSettings(indexSettings)
            .store(store)
            .mergePolicy(NoMergePolicy.INSTANCE)
            .analyzer(new StandardAnalyzer())
            .codecService(codecService)
            .translogConfig(translogConfig)
            .flushMergesAfter(TimeValue.timeValueMinutes(5))
            .externalRefreshListener(externalListeners)
            .internalRefreshListener(internalListeners)
            .globalCheckpointSupplier(() -> SequenceNumbers.NO_OPS_PERFORMED)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .primaryTermSupplier(primaryTerm::get)
            .tombstoneDocSupplier(tombstoneDocSupplier())
            .dataFormatRegistry(registry)
            .mapperService(mapperService)
            .committerFactory(createCommitterFactory(store))
            .build();
    }
}
