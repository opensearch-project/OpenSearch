/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.coordination;

/**
 * Tool to run an unsafe bootstrap
 *
 * @opensearch.internal
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link UnsafeBootstrapClusterManagerCommand}
 */
@Deprecated
public class UnsafeBootstrapMasterCommand extends UnsafeBootstrapClusterManagerCommand {

    UnsafeBootstrapMasterCommand() {
        super();
    }

}
