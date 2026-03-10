/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.exec.coord.Segment;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.FieldAssignmentResolver;
import org.opensearch.index.engine.exec.FieldAssignments;
import org.opensearch.index.engine.exec.FieldSupportRegistry;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeDataFormatWriterPool;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<Any> {

    private final CompositeDataFormatWriterPool dataFormatWriterPool;
    private final Any dataFormat;
    private final AtomicLong writerGeneration;
    private final List<IndexingExecutionEngine<?>> delegates = new ArrayList<>();
    private final FieldSupportRegistry fieldSupportRegistry;
    private final Map<DataFormat, EngineRole> roleMap;
    private final Map<DataFormat, FieldAssignments> fieldAssignmentsMap;

    private static final Logger logger = LogManager.getLogger(CompositeIndexingExecutionEngine.class);

    public CompositeIndexingExecutionEngine(
        EngineConfig engineConfig,
        MapperService mapperService,
        PluginsService pluginsService,
        ShardPath shardPath,
        long initialWriterGeneration,
        IndexSettings indexSettings
    ) {
        this.writerGeneration = new AtomicLong(initialWriterGeneration);
        List<DataSourcePlugin> dataSourcePlugins = pluginsService.filterPlugins(DataSourcePlugin.class)
            .stream().toList();
        if (dataSourcePlugins.isEmpty()) throw new IllegalStateException("No data formats found, can't initialise Engine");

        boolean singlePlugin = dataSourcePlugins.size() == 1;

        // Setting-based role resolution
        String primaryDataFormatName = indexSettings.getValue(IndexSettings.INDEX_COMPOSITE_PRIMARY_DATA_FORMAT_SETTING);
        this.roleMap = resolveRoles(primaryDataFormatName, dataSourcePlugins, singlePlugin);
        logger.debug("[COMPOSITE_DEBUG] Resolved engine roles: {}", roleMap.entrySet().stream()
            .map(e -> e.getKey().name() + " -> " + e.getValue())
            .collect(java.util.stream.Collectors.joining(", ")));

        // Build FieldSupportRegistry from plugin registrations
        this.fieldSupportRegistry = new FieldSupportRegistry();
        for (DataSourcePlugin plugin : dataSourcePlugins) {
            plugin.registerFieldSupport(fieldSupportRegistry);
        }
        logger.debug("[COMPOSITE_DEBUG] FieldSupportRegistry built. Registered formats: {}",
            fieldSupportRegistry.allFormats().stream().map(DataFormat::name).collect(java.util.stream.Collectors.joining(", ")));

        // Validate field capabilities if composite (multiple plugins)
        if (!singlePlugin) {
            CompositeFieldValidator.validatePrimaryCoverage(fieldSupportRegistry, roleMap, mapperService.fieldTypes());
            CompositeFieldValidator.validateMappingPropertyCoverage(fieldSupportRegistry, mapperService.fieldTypes());
            logger.debug("[COMPOSITE_DEBUG] Composite field validation passed for all mapped fields");
        }

        // Resolve field assignments: which format handles which capability for each field
        // Both single-plugin and multi-plugin modes go through per-field resolution
        this.fieldAssignmentsMap = FieldAssignmentResolver.resolve(fieldSupportRegistry, roleMap, mapperService.fieldTypes());
        logger.debug("[COMPOSITE_DEBUG] Resolved per-field assignments for {} format(s)", fieldAssignmentsMap.size());

        // Determine primary format from role map
        DataFormat primaryDataFormat = roleMap.entrySet().stream()
            .filter(e -> e.getValue() == EngineRole.PRIMARY)
            .map(Map.Entry::getKey)
            .findFirst()
            .orElseThrow();

        List<DataFormat> dataFormats = new ArrayList<>();
        for (DataSourcePlugin plugin : dataSourcePlugins) {
            dataFormats.add(plugin.getDataFormat());
            boolean isPrimary = roleMap.get(plugin.getDataFormat()) == EngineRole.PRIMARY;
            FieldAssignments assignments = fieldAssignmentsMap.get(plugin.getDataFormat());
            IndexingExecutionEngine<?> indexingEngine = plugin.indexingEngine(
                engineConfig, mapperService, isPrimary, shardPath, indexSettings, assignments
            );
            delegates.add(indexingEngine);
        }

        this.dataFormat = new Any(dataFormats, primaryDataFormat);

        // logger.debug("Registered dataformats: {}", this.dataFormat);
        this.dataFormatWriterPool = new CompositeDataFormatWriterPool(
            () -> new CompositeDataFormatWriter(this, writerGeneration.getAndIncrement()),
            LinkedList::new,
            Runtime.getRuntime().availableProcessors()
        );
    }

    /**
     * Pure function: resolves engine roles from the primary data format setting.
     * Single plugin → always PRIMARY regardless of setting.
     * Valid setting → matching format is PRIMARY, others SECONDARY.
     * Unknown format name → IllegalArgumentException.
     * Empty setting with multiple plugins → IllegalArgumentException.
     */
    static Map<DataFormat, EngineRole> resolveRoles(
        String primaryDataFormatName,
        List<DataSourcePlugin> plugins,
        boolean singlePlugin
    ) {
        Map<DataFormat, EngineRole> roles = new HashMap<>();
        if (singlePlugin) {
            roles.put(plugins.get(0).getDataFormat(), EngineRole.PRIMARY);
            return roles;
        }
        if (primaryDataFormatName != null && !primaryDataFormatName.isEmpty()) {
            boolean found = false;
            for (DataSourcePlugin plugin : plugins) {
                if (plugin.getDataFormat().name().equals(primaryDataFormatName)) {
                    roles.put(plugin.getDataFormat(), EngineRole.PRIMARY);
                    found = true;
                } else {
                    roles.put(plugin.getDataFormat(), EngineRole.SECONDARY);
                }
            }
            if (!found) {
                throw new IllegalArgumentException(
                    "Unrecognized primary data format [" + primaryDataFormatName + "]. Available: "
                        + plugins.stream().map(p -> p.getDataFormat().name()).toList()
                );
            }
            return roles;
        }
        throw new IllegalArgumentException(
            "index.composite.primary_data_format is required when multiple data formats are registered. Available: "
                + plugins.stream().map(p -> p.getDataFormat().name()).toList()
        );
    }

    public FieldSupportRegistry getFieldSupportRegistry() {
        return fieldSupportRegistry;
    }

    public Map<DataFormat, EngineRole> getRoleMap() {
        return Collections.unmodifiableMap(roleMap);
    }

    public Map<DataFormat, FieldAssignments> getFieldAssignmentsMap() {
        return Collections.unmodifiableMap(fieldAssignmentsMap);
    }


    @Override
    public Any getDataFormat() {
        return dataFormat;
    }

    public long getNextWriterGeneration() {
        return writerGeneration.getAndIncrement();
    }

    /**
     * Updates the writer generation counter to be at least minGeneration + 1.
     * This is used during replication/recovery to ensure the replica's writer generation
     * is always greater than any replicated file's generation, preventing file name collisions.
     *
     * @param minGeneration The minimum generation value from replicated files
     */
    public void updateWriterGenerationIfNeeded(long minGeneration) {
        writerGeneration.updateAndGet(current -> Math.max(current, minGeneration + 1));
    }

    /**
     * Gets the current writer generation without incrementing.
     *
     * @return The current writer generation value
     */
    public long getCurrentWriterGeneration() {
        return writerGeneration.get();
    }

    @Override
    public List<String> supportedFieldTypes(boolean isPrimaryEngine) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadWriterFiles(CatalogSnapshot catalogSnapshot) throws IOException {
        // If this get's called will it not throw exception?
        for (IndexingExecutionEngine<?> delegate : delegates) {
            delegate.loadWriterFiles(catalogSnapshot);
        }
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        for (IndexingExecutionEngine<?> delegate : delegates) {
            // Why creating a map when we are always passing for that format here?
            Map<String, Collection<String>> formatSpecificFilesToDelete = new HashMap<>();
            formatSpecificFilesToDelete.put(delegate.getDataFormat().name(), filesToDelete.get(delegate.getDataFormat().name()));
            delegate.deleteFiles(formatSpecificFilesToDelete);
        }
    }

    @Override
    public Writer<CompositeDataFormatWriter.CompositeDocumentInput> createWriter(long generation) throws IOException {
        throw new UnsupportedOperationException();
    }

    public Writer<CompositeDataFormatWriter.CompositeDocumentInput> createCompositeWriter() {
        return dataFormatWriterPool.getAndLock();
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        RefreshResult finalResult;
        try {
            List<CompositeDataFormatWriter> dataFormatWriters = dataFormatWriterPool.checkoutAll();
            List<Segment> refreshedSegment = refreshInput.getExistingSegments();
            List<Segment> newSegmentList = new ArrayList<>();
            logger.debug("[COMPOSITE_DEBUG] CompositeIndexingExecutionEngine.refresh: flushing {} writers, existing segments={}",
                dataFormatWriters.size(), refreshedSegment.size());
            // flush to disk
            for (CompositeDataFormatWriter dataFormatWriter : dataFormatWriters) {
                Segment newSegment = new Segment(dataFormatWriter.getWriterGeneration());
                FileInfos fileInfos = dataFormatWriter.flush(null);
                fileInfos.getWriterFilesMap().forEach((key, value) -> {
                    logger.debug("[COMPOSITE_DEBUG]   writer gen={} flushed format=[{}] files={}",
                        dataFormatWriter.getWriterGeneration(), key.name(), value.getFiles());
                    newSegment.addSearchableFiles(key.name(), value);
                });
                dataFormatWriter.close();
                if (!newSegment.getDFGroupedSearchableFiles().isEmpty()) {
                    newSegmentList.add(newSegment);
                }
            }

            if (newSegmentList.isEmpty()) {
                logger.debug("[COMPOSITE_DEBUG] No new segments produced from flush");
                return null;
            } else {
                logger.debug("[COMPOSITE_DEBUG] Produced {} new segments from flush", newSegmentList.size());
                refreshedSegment.addAll(newSegmentList);
            }

            // call refresh for delegats
            for (IndexingExecutionEngine<?> delegate : delegates) {
                delegate.refresh(new RefreshInput());
            }

            // make indexing engines aware of everything
            finalResult = new RefreshResult();
            finalResult.setRefreshedSegments(refreshedSegment);

            // provide a view to the upper layer
            return finalResult;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Merger getMerger() {
        throw new UnsupportedOperationException("Merger for Composite Engine is not used");
    }

    public List<IndexingExecutionEngine<?>> getDelegates() {
        return Collections.unmodifiableList(delegates);
    }

    public CompositeDataFormatWriterPool getDataFormatWriterPool() {
        return dataFormatWriterPool;
    }

    public long getNativeBytesUsed() {
        return delegates.stream().mapToLong(IndexingExecutionEngine::getNativeBytesUsed).sum();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(delegates);
    }
}
