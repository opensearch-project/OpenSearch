/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.disruption;

import java.util.Random;

/**
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link BlockClusterManagerServiceOnClusterManager}
 */
@Deprecated
public class BlockMasterServiceOnMaster extends BlockClusterManagerServiceOnClusterManager {
    public BlockMasterServiceOnMaster(Random random) {
        super(random);
    }
}
