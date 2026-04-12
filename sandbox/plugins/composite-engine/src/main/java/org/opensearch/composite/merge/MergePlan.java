/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.merge.OneMerge;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Pre-validated merge plan with per-format file lists and primary/secondary distinction.
 * Segments that predate a format are skipped (null entries filtered).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record MergePlan(
    long mergedWriterGeneration,
    DataFormat primaryFormat,
    List<DataFormat> secondaryFormats,
    Map<DataFormat, List<WriterFileSet>> filesByFormat
) {

    public MergePlan {
        secondaryFormats = List.copyOf(secondaryFormats);
        filesByFormat = Map.copyOf(filesByFormat);
    }

    /** Files for a given format, empty list if the format has no files. */
    public List<WriterFileSet> filesFor(DataFormat format) {
        return filesByFormat.getOrDefault(format, List.of());
    }

    /** Whether this plan has any secondary formats. */
    public boolean hasSecondaries() {
        return secondaryFormats.isEmpty() == false;
    }

    /**
     * Builds a plan from a merge operation, a primary format, secondary formats, and a generation.
     */
    public static MergePlan from(
        OneMerge oneMerge,
        DataFormat primaryFormat,
        List<DataFormat> secondaryFormats,
        long generation
    ) {
        Set<DataFormat> allFormats = new LinkedHashSet<>();
        allFormats.add(primaryFormat);
        allFormats.addAll(secondaryFormats);

        Map<DataFormat, List<WriterFileSet>> filesByFormat = new LinkedHashMap<>();
        for (DataFormat format : allFormats) {
            List<WriterFileSet> files = new ArrayList<>();
            for (Segment segment : oneMerge.getSegmentsToMerge()) {
                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get(format.name());
                if (wfs != null) {
                    files.add(wfs);
                }
            }
            filesByFormat.put(format, List.copyOf(files));
        }
        return new MergePlan(generation, primaryFormat, secondaryFormats, filesByFormat);
    }
}
