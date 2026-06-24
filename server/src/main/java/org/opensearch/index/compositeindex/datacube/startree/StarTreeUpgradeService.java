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
import org.apache.lucene.index.IndexWriter;
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
import org.apache.lucene.store.Lock;
import org.apache.lucene.util.Bits;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Service responsible for retroactively upgrading existing index segments to use Star Tree indexes.
 * <p>
 * The upgrade is a two-phase process:
 * <ol>
 *   <li><b>Phase 1 - Star tree data generation</b>: Iterates over each segment, reads its doc values
 *       via standard Lucene APIs, and builds star tree data structures using the existing
 *       StarTreesBuilder infrastructure. Writes .cid, .cim, .cidvd, .cidvm files via raw IndexOutput.</li>
 *   <li><b>Phase 2 - SegmentInfos and .si rewrite</b>: For each successfully upgraded segment, adds
 *       star tree files to the segment's file set, rewrites the .si file with the expanded file set
 *       (same codec, no codec switch), and commits segments_N+1 atomically. Star tree data is served
 *       via DirectReader cache until a background merge produces a native composite segment.</li>
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

    /** Star tree file extensions created during Phase 1 */
    private static final String[] STAR_TREE_FILE_EXTENSIONS = new String[] {
        Composite912DocValuesFormat.DATA_EXTENSION,
        Composite912DocValuesFormat.META_EXTENSION,
        Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION,
        Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION };

    private StarTreeUpgradeService() {
        // utility class, no instances
    }

    /**
     * Upgrades all eligible segments in the given directory to use star tree indexes.
     * <p>
     * Phase 1: Iterates segments, builds star tree files for those not already using Composite912Codec.
     *          Uses standard Lucene doc values APIs to read dimension/metric data.
     *          Writes .cid, .cim, .cidvd, .cidvm files via raw IndexOutput.
     * Phase 2: Rewrites SegmentInfos and .si files — adds star tree files to each upgraded segment's
     *          file set, rewrites .si files with the expanded file set (same codec), then commits
     *          segments_N+1 atomically.
     *
     * @param directory      the index directory containing the segments to upgrade
     * @param starTreeField  the star tree configuration parsed from the API request body
     * @param mapperService  needed by BaseStarTreeBuilder.generateMetricAggregatorInfos() to resolve FieldValueConverter
     * @return               the number of segments that were upgraded with star tree data
     * @throws IOException   if an I/O error occurs during the upgrade process
     */
    public static int upgradeSegments(Directory directory, StarTreeField starTreeField, MapperService mapperService) throws IOException {
        logger.info("Starting star tree upgrade");
        Set<String> allCandidateSegments = getCandidateSegmentNames(directory);
        Set<String> upgradedSegmentNames;
        try {
            upgradedSegmentNames = buildStarTreeDataForSegments(directory, starTreeField, mapperService);
            if (upgradedSegmentNames.isEmpty() == false) {
                logger.info("Starting Phase 2 — SegmentInfos rewrite for {} upgraded segments", upgradedSegmentNames.size());
                rewriteSegmentInfos(directory, upgradedSegmentNames);
                logger.info("Phase 2 complete — SegmentInfos rewrite finished successfully");
            } else {
                logger.info("No segments were upgraded in Phase 1, skipping Phase 2");
            }
        } catch (Exception e) {
            cleanupStarTreeFiles(directory, allCandidateSegments);
            throw e;
        }
        return upgradedSegmentNames.size();
    }

    /**
     * Returns segment names that are candidates for star tree upgrade (not already using Composite912Codec
     * and having at least one live document).
     */
    public static Set<String> getCandidateSegmentNames(Directory directory) throws IOException {
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        Set<String> candidates = new HashSet<>();
        for (SegmentCommitInfo commitInfo : segmentInfos) {
            if (Composite912Codec.COMPOSITE_INDEX_CODEC_NAME.equals(commitInfo.info.getCodec().getName())) {
                continue;
            }
            int liveDocs = commitInfo.info.maxDoc() - commitInfo.getDelCount() - commitInfo.getSoftDelCount();
            if (liveDocs <= 0) {
                continue;
            }
            candidates.add(commitInfo.info.name);
        }
        return candidates;
    }

    /**
     * Phase 1: Builds star tree data for all eligible segments. Returns set of successfully upgraded segment names.
     */
    public static Set<String> buildStarTreeDataForSegments(Directory directory, StarTreeField starTreeField, MapperService mapperService)
        throws IOException {
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        Set<String> upgradedSegmentNames = ConcurrentHashMap.newKeySet();
        AtomicInteger skippedCount = new AtomicInteger(0);
        AtomicInteger failedCount = new AtomicInteger(0);

        logger.info("Starting star tree Phase 1 for {} segments", segmentInfos.size());

        // Collect eligible segments
        List<SegmentCommitInfo> eligibleSegments = new ArrayList<>();
        for (SegmentCommitInfo commitInfo : segmentInfos) {
            String codecName = commitInfo.info.getCodec().getName();
            if (Composite912Codec.COMPOSITE_INDEX_CODEC_NAME.equals(codecName)) {
                logger.debug("Skipping segment [{}] — already uses Composite912Codec", commitInfo.info.name);
                skippedCount.incrementAndGet();
                continue;
            }
            int liveDocs = commitInfo.info.maxDoc() - commitInfo.getDelCount() - commitInfo.getSoftDelCount();
            if (liveDocs <= 0) {
                logger.debug(
                    "Skipping segment [{}] — no live docs (maxDoc={}, delCount={}, softDelCount={})",
                    commitInfo.info.name,
                    commitInfo.info.maxDoc(),
                    commitInfo.getDelCount(),
                    commitInfo.getSoftDelCount()
                );
                skippedCount.incrementAndGet();
                continue;
            }
            eligibleSegments.add(commitInfo);
        }

        // Build star tree data in parallel across segments
        int parallelism = Math.max(1, Math.min(eligibleSegments.size(), Runtime.getRuntime().availableProcessors() / 2));
        if (parallelism > 1 && eligibleSegments.size() > 1) {
            ExecutorService executor = Executors.newFixedThreadPool(parallelism);
            List<Future<?>> futures = new ArrayList<>();
            for (SegmentCommitInfo commitInfo : eligibleSegments) {
                futures.add(executor.submit(() -> {
                    try {
                        logger.debug("Building star tree data for segment [{}]", commitInfo.info.name);
                        buildStarTreeData(directory, commitInfo, starTreeField, mapperService);
                        upgradedSegmentNames.add(commitInfo.info.name);
                    } catch (Exception e) {
                        failedCount.incrementAndGet();
                        logger.error("Failed to build star tree data for segment [{}]: {}", commitInfo.info.name, e.getMessage(), e);
                    }
                }));
            }
            executor.shutdown();
            try {
                if (executor.awaitTermination(60, TimeUnit.MINUTES) == false) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
                throw new IOException("Star tree build interrupted", e);
            }
            // Check for exceptions
            for (Future<?> future : futures) {
                try {
                    future.get();
                } catch (ExecutionException e) {
                    logger.error("Star tree build task failed: {}", e.getCause().getMessage());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            // Single segment or single core — sequential
            for (SegmentCommitInfo commitInfo : eligibleSegments) {
                try {
                    logger.debug("Building star tree data for segment [{}]", commitInfo.info.name);
                    buildStarTreeData(directory, commitInfo, starTreeField, mapperService);
                    upgradedSegmentNames.add(commitInfo.info.name);
                } catch (Exception e) {
                    failedCount.incrementAndGet();
                    logger.error("Failed to build star tree data for segment [{}]: {}", commitInfo.info.name, e.getMessage(), e);
                }
            }
        }

        logger.info(
            "Phase 1 complete — upgraded: {}, skipped: {}, failed: {} out of {} total segments (parallelism={})",
            upgradedSegmentNames.size(),
            skippedCount.get(),
            failedCount.get(),
            segmentInfos.size(),
            parallelism > 1 ? parallelism : 1
        );
        return upgradedSegmentNames;
    }

    /**
     * Deletes orphaned star tree files for the given segment names. Best-effort, logs warnings on failure.
     */
    public static void cleanupStarTreeFiles(Directory directory, Set<String> segmentNames) {
        for (String segmentName : segmentNames) {
            for (String ext : STAR_TREE_FILE_EXTENSIONS) {
                String fileName = IndexFileNames.segmentFileName(segmentName, "", ext);
                try {
                    directory.deleteFile(fileName);
                } catch (IOException e) {
                    logger.warn("Failed to delete orphaned star tree file [{}]: {}", fileName, e.getMessage());
                } catch (Exception e) {
                    logger.warn("Unexpected error deleting star tree file [{}]: {}", fileName, e.getMessage());
                }
            }
        }
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

            // Build live docs bitset manually (hard + soft deletes) since getLiveDocs() returns
            // null when DirectoryReader wraps with SoftDeletesDirectoryReaderWrapper.
            Bits liveDocs = buildLiveDocsBitset(segmentReader, commitInfo);
            int numLiveDocs = liveDocs != null ? ((org.apache.lucene.util.FixedBitSet) liveDocs).cardinality() : segmentReader.maxDoc();
            if (liveDocs != null) {
                docValuesProducer = new LiveDocsFilteredDocValuesProducer(docValuesProducer, liveDocs, segmentReader.maxDoc());
            }

            // Build fieldProducerMap for all dimensions and metrics
            Map<String, DocValuesProducer> fieldProducerMap = new HashMap<>();
            for (Dimension dimension : starTreeField.getDimensionsOrder()) {
                fieldProducerMap.put(dimension.getField(), docValuesProducer);
            }
            for (Metric metric : starTreeField.getMetrics()) {
                fieldProducerMap.put(metric.getField(), docValuesProducer);
            }
            // _doc_count is an implicit metric expected by StarTreesBuilder.getMetricReaders().
            fieldProducerMap.put(DocCountFieldMapper.NAME, new EmptyDocValuesProducer() {
                @Override
                public NumericDocValues getNumeric(FieldInfo field) {
                    return DocValues.emptyNumeric();
                }
            });

            // Create SegmentWriteState with numLiveDocs (excludes deleted docs) and raw directory.
            FieldInfos fieldInfos = segmentReader.getFieldInfos();
            SegmentInfo segInfo = commitInfo.info;
            SegmentInfo writeSegInfo = new SegmentInfo(
                directory, // raw directory for writing
                segInfo.getVersion(),
                segInfo.getMinVersion(),
                segInfo.name,
                numLiveDocs,
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

            // Consumer write state uses NO_MORE_DOCS for sparse doc values (per Composite912DocValuesWriter pattern).
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
     * Phase 2: Rewrites SegmentInfos to add star tree files to upgraded segments' file sets.
     * <p>
     * For each segment in upgradedSegmentNames:
     *   1. Add star tree files (.cid, .cim, .cidvd, .cidvm) to the segment's file set
     *   2. Rewrite the .si file with the expanded file set (same codec, no codec switch)
     * Segments NOT in upgradedSegmentNames are left untouched.
     * Commits segments_N+1 atomically.
     * <p>
     * The original codec is preserved on all segments. Star tree data is served via
     * DirectReader cache until a background merge produces a native composite segment.
     *
     * @param directory             the index directory
     * @param upgradedSegmentNames  the set of segment names that were successfully upgraded in Phase 1
     * @throws IOException          if an I/O error occurs during the SegmentInfos rewrite
     */

    /**
     * Builds a live docs bitset for a segment by combining hard deletes (.liv file) and
     * soft deletes (__soft_deletes doc values field). Returns null if all docs are live.
     */
    private static Bits buildLiveDocsBitset(SegmentReader segmentReader, SegmentCommitInfo commitInfo) throws IOException {
        int maxDoc = segmentReader.maxDoc();
        int hardDeleteCount = commitInfo.getDelCount();
        int softDeleteCount = commitInfo.getSoftDelCount();

        logger.debug(
            "buildLiveDocsBitset: segment={} hardDel={} softDel={} maxDoc={}",
            commitInfo.info.name,
            hardDeleteCount,
            softDeleteCount,
            maxDoc
        );

        if (hardDeleteCount == 0 && softDeleteCount == 0) {
            return null;
        }

        org.apache.lucene.util.FixedBitSet liveBits = new org.apache.lucene.util.FixedBitSet(maxDoc);
        liveBits.set(0, maxDoc);

        // Apply hard deletes from .liv file
        Bits hardLiveDocs = segmentReader.getLiveDocs();
        if (hardLiveDocs != null) {
            for (int i = 0; i < maxDoc; i++) {
                if (hardLiveDocs.get(i) == false) {
                    liveBits.clear(i);
                }
            }
        }

        // Apply soft deletes via segmentReader.getNumericDocValues() which routes to the update file.
        String softDeleteField = org.opensearch.common.lucene.Lucene.SOFT_DELETES_FIELD;
        if (softDeleteCount > 0) {
            NumericDocValues softDeleteValues = segmentReader.getNumericDocValues(softDeleteField);
            if (softDeleteValues != null) {
                int docId;
                while ((docId = softDeleteValues.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    if (softDeleteValues.longValue() == 1) {
                        liveBits.clear(docId);
                    }
                }
                logger.debug("buildLiveDocsBitset: segment={} liveBits.cardinality={}", commitInfo.info.name, liveBits.cardinality());
            } else {
                logger.warn(
                    "buildLiveDocsBitset: segment={} __soft_deletes field returned NULL from getNumericDocValues",
                    commitInfo.info.name
                );
            }
        }

        return liveBits;
    }

    public static void rewriteSegmentInfos(Directory directory, Set<String> upgradedSegmentNames) throws IOException {
        Lock writeLock = directory.obtainLock(IndexWriter.WRITE_LOCK_NAME);
        try {
            SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);

            for (SegmentCommitInfo commitInfo : segmentInfos) {
                String segName = commitInfo.info.name;

                if (upgradedSegmentNames.contains(segName) == false) {
                    continue;
                }

                // Add star tree files to the existing file set (no codec switch).
                // Star tree data is served via DirectReader cache until a background merge
                // produces a native composite segment.
                SegmentInfo segInfo = commitInfo.info;
                Set<String> files = new HashSet<>(segInfo.files());
                files.add(IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.DATA_EXTENSION));
                files.add(IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.META_EXTENSION));
                files.add(IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.DATA_DOC_VALUES_EXTENSION));
                files.add(IndexFileNames.segmentFileName(segName, "", Composite912DocValuesFormat.META_DOC_VALUES_EXTENSION));
                segInfo.setFiles(files);

                // Rewrite .si file with expanded file set (same codec, same diagnostics)
                String siFileName = IndexFileNames.segmentFileName(segName, "", "si");
                directory.deleteFile(siFileName);
                segInfo.getCodec().segmentInfoFormat().write(directory, segInfo, IOContext.DEFAULT);
            }

            // Commit segments_N+1 atomically (generation auto-incremented by commit())
            segmentInfos.commit(directory);
            directory.sync(segmentInfos.files(true));
            directory.syncMetaData();

            logger.info(
                "SegmentInfos rewrite complete — committed new segment infos for {} upgraded segments. " + "Generation: {}, files: {}",
                upgradedSegmentNames.size(),
                segmentInfos.getGeneration(),
                segmentInfos.files(true)
            );
        } finally {
            writeLock.close();
        }
    }
}
