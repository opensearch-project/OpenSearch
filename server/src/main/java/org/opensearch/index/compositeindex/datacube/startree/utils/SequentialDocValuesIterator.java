
/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.apache.lucene.util.LongValues;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;

import java.io.IOException;

/**
 * Coordinates the reading of documents across multiple StarTreeValuesIterator.
 * It encapsulates a single StarTreeValuesIterator and maintains the latest document ID and its associated value.
 *
 * In case of merge , this will be reading the entries of star tree values and in case of flush this will go through
 * the actual segment documents.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class SequentialDocValuesIterator {

    /**
     * The doc id set iterator associated for each field.
     */
    private final StarTreeValuesIterator starTreeValuesIterator;

    /**
     * The id of the latest record/entry.
     */
    private int entryId = -1;

    public SequentialDocValuesIterator(StarTreeValuesIterator starTreeValuesIterator) {
        this.starTreeValuesIterator = starTreeValuesIterator;
    }

    /**
     * Returns the ID of the star tree record/entry or the segment document id
     *
     * @return the ID of the star tree record/entry or the segment document id
     */
    public int getEntryId() {
        return entryId;
    }

    /**
     * Sets the id of the latest entry.
     *
     * @param entryId the ID of the star tree record/entry or the segment document id
     */
    private void setEntryId(int entryId) {
        this.entryId = entryId;
    }

    public int nextEntry(int currentEntryId) throws IOException {
        // if doc id stored is less than or equal to the requested doc id , return the stored doc id
        if (entryId >= currentEntryId) {
            return entryId;
        }
        setEntryId(this.starTreeValuesIterator.nextEntry());
        return entryId;
    }

    public Long value(int currentEntryId) throws IOException {
        if (starTreeValuesIterator instanceof SortedNumericStarTreeValuesIterator) {
            if (currentEntryId < 0) {
                throw new IllegalStateException("invalid entry id to fetch the next value");
            }
            if (currentEntryId == StarTreeValuesIterator.NO_MORE_ENTRIES) {
                throw new IllegalStateException("StarTreeValuesIterator is already exhausted");
            }
            if (entryId == StarTreeValuesIterator.NO_MORE_ENTRIES || entryId != currentEntryId) {
                return null;
            }
            return ((SortedNumericStarTreeValuesIterator) starTreeValuesIterator).nextValue();

        } else if (starTreeValuesIterator instanceof SortedSetStarTreeValuesIterator) {
            if (currentEntryId < 0) {
                throw new IllegalStateException("invalid entry id to fetch the next value");
            }
            if (currentEntryId == StarTreeValuesIterator.NO_MORE_ENTRIES) {
                throw new IllegalStateException("StarTreeValuesIterator is already exhausted");
            }
            if (entryId == StarTreeValuesIterator.NO_MORE_ENTRIES || entryId != currentEntryId) {
                return null;
            }
            return ((SortedSetStarTreeValuesIterator) starTreeValuesIterator).nextOrd();
        } else {
            throw new IllegalStateException("Unsupported Iterator requested for SequentialDocValuesIterator");
        }
    }

    public Long value(int currentEntryId, LongValues globalOrdinalLongValues) throws IOException {
        if (starTreeValuesIterator instanceof SortedNumericStarTreeValuesIterator) {
            return value(currentEntryId);
        } else if (starTreeValuesIterator instanceof SortedSetStarTreeValuesIterator) {
            assert globalOrdinalLongValues != null;
            Long val = value(currentEntryId);
            // convert local ordinal to global ordinal
            if (val != null) {
                val = globalOrdinalLongValues.get(val);
            }
            return val;
        } else {
            throw new IllegalStateException("Unsupported Iterator requested for SequentialDocValuesIterator");
        }
    }
}
