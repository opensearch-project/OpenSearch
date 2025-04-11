/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.tasks;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

public class SearchTaskCancellationStatsTests extends AbstractWireSerializingTestCase<SearchTaskCancellationStats> {
    @Override
    protected Writeable.Reader<SearchTaskCancellationStats> instanceReader() {
        return SearchTaskCancellationStats::new;
    }

    @Override
    protected SearchTaskCancellationStats createTestInstance() {
        return randomInstance();
    }

    public static SearchTaskCancellationStats randomInstance() {
        return new SearchTaskCancellationStats(randomNonNegativeLong(), randomNonNegativeLong());
    }
}
