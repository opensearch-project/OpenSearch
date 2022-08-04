/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.disruption;

import org.opensearch.common.Priority;

import java.util.Random;

/**
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link BusyClusterManagerServiceDisruption}
 */
@Deprecated
public class BusyMasterServiceDisruption extends BusyClusterManagerServiceDisruption {
    public BusyMasterServiceDisruption(Random random, Priority priority) {
        super(random, priority);
    }
}
