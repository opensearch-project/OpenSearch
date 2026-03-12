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
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.codec.composite.composite912.Composite912Codec;

import java.io.IOException;

/**
 * Service responsible for retroactively upgrading existing index segments to use Star Tree indexes.
 * <p>
 * The upgrade is a two-phase process:
 * <ol>
 *   <li><b>Phase 1 - Star tree data generation</b>: Iterates over each segment, reads its doc values,
 *       and builds star tree data structures using the existing StarTreesBuilder infrastructure.</li>
 *   <li><b>Phase 2 - Codec upgrade via IndexUpgrader</b>: Uses Lucene's IndexUpgrader with an
 *       IndexWriterConfig configured via setCodec(Composite912Codec) to rewrite segment metadata
 *       so all segments declare Composite912Codec.</li>
 * </ol>
 * <p>
 * The star tree field configuration (dimensions, metrics) is provided directly as a {@link StarTreeField}
 * parameter, NOT read from MapperService. This allows upgrading indexes that were created without
 * {@code index.composite_index: true}.
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
     * Phase 2: Runs IndexUpgrader with IWC.setCodec(Composite912Codec) to rewrite segment metadata.
     *
     * @param directory      the index directory containing the segments to upgrade
     * @param starTreeField  the star tree configuration parsed from the API request body
     * @return               the number of segments that were upgraded with star tree data
     * @throws IOException   if an I/O error occurs during the upgrade process
     */
    public static int upgradeSegments(Directory directory, StarTreeField starTreeField) throws IOException {
        SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
        int upgradedCount = 0;
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
                buildStarTreeData(directory, commitInfo, starTreeField);
                upgradedCount++;
            } catch (Exception e) {
                failedCount++;
                logger.error("Failed to build star tree data for segment [{}]: {}", commitInfo.info.name, e.getMessage(), e);
            }
        }

        logger.info(
            "Phase 1 complete — upgraded: {}, skipped: {}, failed: {} out of {} total segments",
            upgradedCount,
            skippedCount,
            failedCount,
            segmentInfos.size()
        );

        // Phase 2: Run IndexUpgrader to switch all segments to Composite912Codec
        if (upgradedCount > 0) {
            logger.info("Starting Phase 2 — codec upgrade via IndexUpgrader for {} upgraded segments", upgradedCount);
            upgradeCodec(directory);
            logger.info("Phase 2 complete — codec upgrade finished successfully");
        } else {
            logger.info("No segments were upgraded in Phase 1, skipping Phase 2 codec upgrade");
        }

        return upgradedCount;
    }

    /**
     * Phase 1: Builds star tree files for a single segment.
     * <p>
     * Opens a SegmentReader, builds fieldProducerMap from starTreeField dimensions + metrics,
     * creates SegmentWriteState, and uses StarTreesBuilder to write .cid, .cim, .cidvd, .cidvm files.
     * <p>
     * Does NOT modify the segment's codec or .si file — that's handled by IndexUpgrader in Phase 2.
     *
     * @param directory      the index directory
     * @param commitInfo     the segment commit info for the segment to process
     * @param starTreeField  the star tree configuration
     * @throws IOException   if an I/O error occurs during star tree data generation
     */
    static void buildStarTreeData(Directory directory, SegmentCommitInfo commitInfo, StarTreeField starTreeField) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /**
     * Phase 2: Runs IndexUpgrader to switch all segments to Composite912Codec.
     * <p>
     * Creates an IndexWriterConfig, calls iwc.setCodec(new Composite912Codec()),
     * and passes it to new IndexUpgrader(directory, iwc).upgrade().
     * This rewrites .si files, updates file sets to include star tree files,
     * and commits segments_N+1.
     *
     * @param directory  the index directory to upgrade
     * @throws IOException if an I/O error occurs during the codec upgrade
     */
    static void upgradeCodec(Directory directory) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
