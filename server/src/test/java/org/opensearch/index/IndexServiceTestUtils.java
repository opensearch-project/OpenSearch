/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractAsyncTask;

public final class IndexServiceTestUtils {
    private IndexServiceTestUtils() {}

    public static void setTrimTranslogTaskInterval(IndexService indexService, TimeValue interval) {
        ((AbstractAsyncTask) indexService.getTrimTranslogTask()).setInterval(interval);
    }
}
