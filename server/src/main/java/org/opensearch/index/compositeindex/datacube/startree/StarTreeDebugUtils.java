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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;

/**
 * Debug utility for diagnosing star tree ordinal mismatches.
 * Activate with -Dopensearch.startree.debug=true
 * DELETE THIS FILE after debugging is complete.
 */
public class StarTreeDebugUtils {

    private static final Logger logger = LogManager.getLogger(StarTreeDebugUtils.class);

    public static void dumpOrdinalSpaces(
        String segmentName,
        String fieldName,
        SortedSetDocValues starTreeOrdinals,
        SortedSetDocValues segmentOrdinals
    ) throws IOException {
        logger.info("=== ORDINAL DUMP: segment={} field={} ===", segmentName, fieldName);

        if (starTreeOrdinals == null) {
            logger.info("  starTree ordinals: NULL");
        } else {
            logger.info("  starTree valueCount={}", starTreeOrdinals.getValueCount());
            logger.info("  --- Star Tree Ordinals ---");
            for (long i = 0; i < Math.min(starTreeOrdinals.getValueCount(), 50); i++) {
                BytesRef term = starTreeOrdinals.lookupOrd(i);
                logger.info("    starOrd={} term={}", i, term.utf8ToString());
            }
        }

        if (segmentOrdinals == null) {
            logger.info("  segment ordinals: NULL");
        } else {
            logger.info("  segment valueCount={}", segmentOrdinals.getValueCount());
            logger.info("  --- Segment Ordinals ---");
            for (long i = 0; i < Math.min(segmentOrdinals.getValueCount(), 50); i++) {
                BytesRef term = segmentOrdinals.lookupOrd(i);
                logger.info("    segOrd={} term={}", i, term.utf8ToString());
            }
        }

        if (starTreeOrdinals != null && segmentOrdinals != null) {
            logger.info("  --- Translation (starOrd → segOrd) ---");
            for (long i = 0; i < Math.min(starTreeOrdinals.getValueCount(), 50); i++) {
                BytesRef term = starTreeOrdinals.lookupOrd(i);
                long segOrd = segmentOrdinals.lookupTerm(term);
                logger.info("    starOrd={} term={} → segOrd={} {}",
                    i, term.utf8ToString(), segOrd,
                    segOrd < 0 ? "*** MISSING ***" : "OK");
            }
        }
    }
}
