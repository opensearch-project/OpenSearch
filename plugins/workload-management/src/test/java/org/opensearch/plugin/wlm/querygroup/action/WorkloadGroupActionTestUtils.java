/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.action;

import org.opensearch.wlm.MutableWorkloadGroupFragment;

public class WorkloadGroupActionTestUtils {
    public static UpdateWorkloadGroupRequest updateWorkloadGroupRequest(
        String name,
        MutableWorkloadGroupFragment mutableWorkloadGroupFragment
    ) {
        return new UpdateWorkloadGroupRequest(name, mutableWorkloadGroupFragment);
    }
}
