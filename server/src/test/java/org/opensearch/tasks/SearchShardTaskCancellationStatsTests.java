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

public class SearchShardTaskCancellationStatsTests extends AbstractWireSerializingTestCase<SearchShardTaskCancellationStats> {
    @Override
    protected Writeable.Reader<SearchShardTaskCancellationStats> instanceReader() {
        return SearchShardTaskCancellationStats::new;
    }

    @Override
    protected SearchShardTaskCancellationStats createTestInstance() {
        return randomInstance();
    }

    public static SearchShardTaskCancellationStats randomInstance() {
        return new SearchShardTaskCancellationStats(randomNonNegativeLong(), randomNonNegativeLong());
    }
}
