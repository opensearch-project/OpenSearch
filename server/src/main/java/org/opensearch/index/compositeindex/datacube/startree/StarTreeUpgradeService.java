/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.codec.composite.LuceneDocValuesConsumerFactory;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;
import org.opensearch.index.codec.composite.composite912.Composite912DocValuesFormat;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.startree.builder.StarTreesBuilder;
import org.opensearch.index.mapper.DocCountFieldMapper;
import org.opensearch.index.mapper.MapperService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service responsible for retroactively upgrading existing index segments to use Star Tree indexes.
 * <p>
 * The upgrade is a two-phase process:
 * <ol>
 *   <li><b>Phase 1 - Star tree data generation</b>: Iterates over each segment, reads its doc values
 *       via standard Lucene APIs, and builds star tree data structures using the existing
 *       StarTreesBuilder infrastructure. Writes .cid, .cim, .cidvd, .cidvm files via raw IndexOutput.</li>
 *   <li><b>Phase 2 - Codec switch via direct SegmentInfos rewrite</b>: For each successfully upgraded
 *       segment, constructs a new SegmentCommitInfo with Composite912Codec and the star tree files
 *       added to the file set. Commits segments_N+1 atomically. Follows the same pattern as
 *       Store.commitSegmentInfos().</li>
 * </ol>
 * <p>
 * The star tree field configuration (dimensions, metrics) is provided directly as a {@link StarTreeField}
 * parameter. {@link MapperService} is required by BaseStarTreeBuilder.generateMetricAggregatorInfos()
 * to resolve FieldValueConverter for each metric field. The mapping update must happen before the
 * per-shard upgrade so that MapperService has the composite field types available.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class StarTreeUpgradeService {

    private static final Logger logger = LogManager.getLogger(StarTreeUpgradeService.class);

    private StarTreeUpgradeService() {
        // utility class, no instances
    }

    /**
     * Upgrades all eligible segments in the given directory to use star tree indexes.
     * <p>
     * Phase 1: Iterates segments, builds star tree files for those not already using Composite912Codec.
     *          Uses standard Lucene doc values APIs to read dimension/metric data.
     *          Writes .cid, .cim, .cidvd, .cidvm files via raw IndexOutput.
     * Phase 2: Rewrites SegmentInfos directly — creates new SegmentCommitInfo objects with
     *          Composite912Codec and star tree files in the file set, then commits segments_N+1.
     *
     * @param directory      the index directory containing the segments to upgrade
     * @param starTreeField  the star tree configuration parsed from the API request body
     * @param mapperService  needed by BaseStarTreeBuilder.generateMetricAggregatorInfos() to resolve FieldValueConverter
     * @return               the number of segments that were upgraded with star tree data
     * @throws IOException   if an I/O error occurs during the upgrade process
     */
    public static int upgradeSegments(Directory directory, StarTreeField starTreeField, MapperService mapperService) throws IOException {
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        Set<String> upgradedSegmentNames = new HashSet<>();
        int skippedCount = 0;
        int failedCount = 0;

        logger.info("Starting star tree upgrade for {} segments", segmentInfos.size());

        // Phase 1: Build star tree data for each eligible segment
        for (SegmentCommitInfo commitInfo : segmentInfos) {
            String codecName = commitInfo.info.getCodec().getName();
            if (Composite912Codec.COMPOSITE_INDEX_CODEC_NAME.equals(codecName)) {
                logger.debug("Skipping segment [{}] — already uses Composite912Codec", commitInfo.info.name);
                skippedCount++;
                continue;
            }

            try {
                logger.debug("Building star tree data for segment [{}] with codec [{}]", commitInfo.info.name, codecName);
                buildStarTreeData(directory, commitInfo, starTreeField, mapperService);
                upgradedSegmentNames.add(commitInfo.info.name);
            } catch (Exception e) {
                failedCount++;
                logger.error("Failed to build star tree data for segment [{}]: {}", commitInfo.info.name, e.getMessage(), e);
            }
        }

        logger.info(
            "Phase 1 complete — upgraded: {}, skipped: {}, failed: {} out of {} total segments",
            upgradedSegmentNames.size(),
            skippedCount,
            failedCount,
            segmentInfos.size()
        );

        // Phase 2: Rewrite SegmentInfos to switch upgraded segments to Composite912Codec
        if (upgradedSegmentNames.isEmpty() == false) {
            logger.info("Starting Phase 2 — SegmentInfos rewrite for {} upgraded segments", upgradedSegmentNames.size());
            rewriteSegmentInfos(directory, upgradedSegmentNames);
            logger.info("Phase 2 complete — SegmentInfos rewrite finished successfully");
        } else {
            logger.info("No segments were upgraded in Phase 1, skipping Phase 2 SegmentInfos rewrite");
        }

        return upgradedSegmentNames.size();
    }

    /**
     * Phase 1: Builds star tree files for a single segment.
     * <p>
     * Opens a DirectoryReader to find the matching SegmentReader for the given segment.
     * Gets DocValuesProducer from the reader. Builds fieldProducerMap for all dimensions
     * and metrics. Creates SegmentWriteState. Opens IndexOutput for .cid/.cim and
     * DocValuesConsumer for .cidvd/.cidvm. Calls StarTreesBuilder.build().
     * <p>
     * Does NOT modify the segment's codec or .si file — that's Phase 2.
     *
     * @param directory      the index directory
     * @param commitInfo     the segment commit info for the segment to process
     * @param starTreeField  the star tree configuration
     * @param mapperService  needed by BaseStarTreeBuilder.generateMetricAggregatorInfos()
     * @throws IOException   if an I/O error occurs during star tree data generation
     */
    static void buildStarTreeData(
        Directory directory,
        SegmentCommitInfo commitInfo,
        StarTreeField starTreeField,
        MapperService mapperService
    ) throws IOException {
        String segmentName = commitInfo.info.name;
        DirectoryReader directoryReader = null;
        IndexOutput dataOut = null;
        IndexOutput metaOut = null;
        DocValuesConsumer compositeDocValuesConsumer = null;

        try {
            // Open a DirectoryReader and find the matching SegmentReader by segment name
            directoryReader = DirectoryReader.open(directory);
            SegmentReader segmentReader = null;
            for (LeafReaderContext leafContext : directoryReader.leaves()) {
                SegmentReader candidate = Lucene.segmentReader(leafContext.reader());
                if (candidate.getSegmentName().equals(segmentName)) {
                    segmentReader = candidate;
                    break;
                }
            }
            if (segmentReader == null) {
                throw new IOException("Could not find SegmentReader for segment [" + segmentName + "]");
            }

            // Get DocValuesProducer from the reader
            DocValuesProducer docValuesProducer = segmentReader.getDocValuesReader();
            if (docValuesProducer == null) {
                throw new IOException("No DocValuesProducer available for segment [" + segmentName + "]");
            }

            // Build fieldProducerMap for all dimensions and metrics
            Map<String, DocValuesProducer> fieldProducerMap = new HashMap<>();
            for (Dimension dimension : starTreeField.getDimensionsOrder()) {
                fieldProducerMap.put(dimension.getField(), docValuesProducer);
            }
            for (Metric metric : starTreeField.getMetrics()) {
                fieldProducerMap.put(metric.getField(), docValuesProducer);
            }
            // Add _doc_count with empty NumericDocValues producer — StarTreesBuilder always
            // includes _doc_count as an implicit metric, and getMetricReaders() expects it
            // in the fieldProducerMap. Following Composite912DocValuesWriter.addDocValuesForEmptyField().
            fieldProducerMap.put(DocCountFieldMapper.NAME, new EmptyDocValuesProducer() {
                @Override
                public NumericDocValues getNumeric(FieldInfo field) {
                    return DocValues.emptyNumeric();
                }
            });

            // Create SegmentWriteState with the segment's actual maxDoc
            // Use the raw directory (not compound) for writing star tree files
            FieldInfos fieldInfos = segmentReader.getFieldInfos();
            SegmentInfo segInfo = commitInfo.info;
            SegmentInfo writeSegInfo = new SegmentInfo(
                directory, // raw directory for writing
                segInfo.getVersion(),
                segInfo.getMinVersion(),
                segInfo.name,
                segInfo.maxDoc(),
                false, // useCompoundFile = false for writing
                segInfo.getHasBlocks(),
                segInfo.getCodec(),
                segInfo.getDiagnostics(),
                segInfo.getId(),
                segInfo.getAttributes(),
                segInfo.getIndexSort()
            );
            SegmentWriteState state = new SegmentWriteState(
                null, // infoStream
                directory,
                writeSegInfo,
                fieldInfos,
                null, // segUpdates
                IOContext.DEFAULT,
                "" // segmentSuffix
            );

            // Open IndexOutput for .cid and .cim files with proper CodecUtil headers
            String dataFileName = IndexFileNames.segmentFileName(segmentName, "", Composite912DocValuesFormat.DATA_EXTENSION);
            dataOut = directory.createOutput(dataFileName, IOContext.DEFAULT);
            CodecUtil.writeIndexHeader(
                dataOut,
                Composite912DocValuesFormat.DATA_CODEC_NAME,
                Composite912DocValuesFormat.VERSION_CURRENT,
                segInfo.getId(),
                ""
            );

            String metaFileName = IndexFileNames.segmentFileName(segmentName, "", Composite912DocValuesFormat.META_EXTENSION);
            metaOut = directory.createOutput(metaFileName, IOContext.DEFAULT);
            CodecUtil.writeIndexHeader(
                metaOut,
                Composite912DocValuesFormat.META_CODEC_NAME,
                Composite912DocValuesFormat.VERSION_CURRENT,
                segInfo.getId(),
                ""
            );

            // Create a consumer write state with DocIdSetIterator.NO_MORE_DOCS for sparse doc values
            // (following the pattern in Composite912DocValuesWriter.getSegmentWriteState())
            // Use the same segment name and ID so file names match the segment
            SegmentInfo consumerSegInfo = new SegmentInfo(
                directory, // use raw directory, not compound directory
                segInfo.getVersion(),
                segInfo.getMinVersion(),
                segInfo.name,
                DocIdSetIterator.NO_MORE_DOCS,
                false, // useCompoundFile = false for writing to raw directory
                segInfo.getHasBlocks(),
                segInfo.getCodec(),
                segInfo.getDiagnostics(),
                segInfo.getId(),
                segInfo.getAttributes(),
                segInfo.getIndexSort()
            );
            SegmentWriteState consumerWriteState = new SegmentWriteState(
                null,
                directory,
                consumerSegInfo,
                fieldInfos,
                null,
                IOContext.DEFAULT,
                ""
            );

            // Create DocValuesConsumer for .cidvd and .cidvm files
            compositeDocValuesConsumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
                consumerWriteState,
                4096, /* Lucene90DocValuesFormat#DEFAULT_SKIP_INDEX_INTERVAL_SIZE */
                Composite912DocValuesFormat.DATA_DOC_VALUES_CODEC,
                Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
                Composite912DocValuesFormat.META_DOC_VALUES_CODEC,
                Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION
            );

            // Build star tree data using StarTreesBuilder
            try (StarTreesBuilder starTreesBuilder = new StarTreesBuilder(state, mapperService, new AtomicInteger())) {
                starTreesBuilder.build(metaOut, dataOut, fieldProducerMap, compositeDocValuesConsumer);
            }

            // Write EOF marker and CodecUtil footer (following Composite912DocValuesWriter.close() pattern)
            metaOut.writeLong(-1); // EOF marker
            CodecUtil.writeFooter(metaOut);
            CodecUtil.writeFooter(dataOut);

            logger.info(
                "Star tree files written for segment [{}]: directory listing = {}",
                segmentName,
                java.util.Arrays.toString(directory.listAll())
            );

        } finally {
            // Close everything in finally blocks to prevent resource leaks
            if (compositeDocValuesConsumer != null) {
                try {
                    compositeDocValuesConsumer.close();
                } catch (Exception e) {
                    logger.warn("Failed to close DocValuesConsumer for segment [{}]", segmentName, e);
                }
            }
            if (metaOut != null) {
                try {
                    metaOut.close();
                } catch (Exception e) {
                    logger.warn("Failed to close meta IndexOutput for segment [{}]", segmentName, e);
                }
            }
            if (dataOut != null) {
                try {
                    dataOut.close();
                } catch (Exception e) {
                    logger.warn("Failed to close data IndexOutput for segment [{}]", segmentName, e);
                }
            }
            if (directoryReader != null) {
                try {
                    directoryReader.close();
                } catch (Exception e) {
                    logger.warn("Failed to close DirectoryReader for segment [{}]", segmentName, e);
                }
            }
        }
    }

    /**
     * Phase 2: Rewrites SegmentInfos to switch upgraded segments to Composite912Codec.
     * <p>
     * For each segment in upgradedSegmentNames:
     *   1. Create new SegmentInfo with Composite912Codec (copy all other fields from original)
     *   2. Add star tree files (.cid, .cim, .cidvd, .cidvm) to the file set
     *   3. Create new SegmentCommitInfo preserving delCount/softDelCount/delGen/fieldInfosGen/docValuesGen/id
     * Segments NOT in upgradedSegmentNames are kept unchanged.
     * Commits segments_N+1 atomically.
     * <p>
     * Follows the same pattern as Store.commitSegmentInfos() (Store.java line 907).
     *
     * @param directory             the index directory
     * @param upgradedSegmentNames  the set of segment names that were successfully upgraded in Phase 1
     * @throws IOException          if an I/O error occurs during the SegmentInfos rewrite
     */
    static void rewriteSegmentInfos(Directory directory, Set<String> upgradedSegmentNames) throws IOException {
        SegmentInfos originalInfos = SegmentInfos.readLatestCommit(directory);
        SegmentInfos newSegmentInfos = originalInfos.clone();
        newSegmentInfos.clear();

        for (SegmentCommitInfo commitInfo : originalInfos) {
            if (upgradedSegmentNames.contains(commitInfo.info.name)) {
                SegmentInfo oldInfo = commitInfo.info;

                // Create new SegmentInfo with Composite912Codec, copying all other fields.
                // Keep useCompoundFile as-is — the original segment data stays in .cfs.
                // Star tree files (.cid, .cim, .cidvd, .cidvm) are outside .cfs, and
                // Composite912DocValuesReader falls back to segmentInfo.dir when it can't
                // find them in the CompoundDirectory.
                SegmentInfo newInfo = new SegmentInfo(
                    oldInfo.dir,
                    oldInfo.getVersion(),
                    oldInfo.getMinVersion(),
                    oldInfo.name,
                    oldInfo.maxDoc(),
                    oldInfo.getUseCompoundFile(),
                    oldInfo.getHasBlocks(),
                    new Composite912Codec(),
                    oldInfo.getDiagnostics(),
                    oldInfo.getId(),
                    oldInfo.getAttributes(),
                    oldInfo.getIndexSort()
                );

                // Add star tree files to the file set (original files + star tree files)
                Set<String> files = new HashSet<>(oldInfo.files());
                String segName = oldInfo.name;
                files.add(IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.DATA_EXTENSION));
                files.add(IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.META_EXTENSION));
                files.add(IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION));
                files.add(IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION));
                newInfo.setFiles(files);

                // Assertions to verify no state is lost
                assert newInfo.maxDoc() == oldInfo.maxDoc();
                assert newInfo.getVersion().equals(oldInfo.getVersion());
                assert Arrays.equals(newInfo.getId(), oldInfo.getId());
                assert Objects.equals(newInfo.getIndexSort(), oldInfo.getIndexSort());
                assert newInfo.getAttributes().equals(oldInfo.getAttributes());

                // Create new SegmentCommitInfo preserving all commit metadata
                SegmentCommitInfo newCommitInfo = new SegmentCommitInfo(
                    newInfo,
                    commitInfo.getDelCount(),
                    commitInfo.getSoftDelCount(),
                    commitInfo.getDelGen(),
                    commitInfo.getFieldInfosGen(),
                    commitInfo.getDocValuesGen(),
                    commitInfo.getId()
                );

                newSegmentInfos.add(newCommitInfo);

                // Rewrite the .si file with Composite912Codec's SegmentInfoFormat
                // The original .si was written by the old codec — we need to rewrite it
                // so it declares Composite912Codec. Without this, the IndexWriter reads
                // the old codec name from .si and ignores our SegmentInfos codec change.
                String siFileName = IndexFileNames.segmentFileName(segName, "", "si");
                directory.deleteFile(siFileName);
                new Composite912Codec().segmentInfoFormat().write(directory, newInfo, IOContext.DEFAULT);
            } else {
                // Keep original SegmentCommitInfo unchanged
                newSegmentInfos.add(commitInfo);
            }
        }

        // Copy user data from original SegmentInfos
        newSegmentInfos.setUserData(originalInfos.getUserData(), false);

        // Commit segments_N+1 atomically (generation auto-incremented by commit())
        newSegmentInfos.commit(directory);
        directory.sync(newSegmentInfos.files(true));
        directory.syncMetaData();

        logger.info(
            "SegmentInfos rewrite complete — committed new segment infos for {} upgraded segments. "
                + "Generation: {}, files: {}, directory listing: {}",
            upgradedSegmentNames.size(),
            newSegmentInfos.getGeneration(),
            newSegmentInfos.files(true),
            java.util.Arrays.toString(directory.listAll())
        );
    }
}
