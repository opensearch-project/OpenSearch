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
import org.opensearch.index.engine.exec.WriterFileSet;

import java.util.List;

/**
 * One side table's inputs to a merge: the files to merge, and the format that will do the merging.
 *
 * <p>The two names are deliberately separate. {@code auxiliaryFormatName} is the <em>logical</em>
 * identity — the key the catalog holds the side table's files under, and the key the merged segment
 * must come back out under. {@code storageFormat} is the <em>physical</em> identity — the format
 * that actually owns the file layout and the merge kernel, obtained via
 * {@link DataFormat#storageFormat()}. A nested child table is a parquet file merged by the parquet
 * merger, and the merger looks its inputs up by its own name, so the {@code MergeInput} handed to it
 * must be keyed {@code parquet} while the resulting {@code Segment} is keyed
 * {@code aux__parquet__nested}. Conflating the two is the recurring bug in this area: a physical
 * operation routed by the catalog format name.
 *
 * @param auxiliaryFormatName the catalog key for this side table, e.g. {@code aux__parquet__nested}
 * @param storageFormat       the format whose merger and file layout the side table reuses
 * @param files               the side table files to merge, in catalog order
 * @opensearch.experimental
 */
@ExperimentalApi
public record AuxiliaryMergeGroup(String auxiliaryFormatName, DataFormat storageFormat, List<WriterFileSet> files) {

    public AuxiliaryMergeGroup {
        files = List.copyOf(files);
    }
}
