/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.persistent.decider;

import org.opensearch.cluster.ClusterState;
import org.opensearch.common.settings.Settings;
import org.opensearch.persistent.PersistentTasksDecidersTestCase;

public class EnableAssignmentDeciderTests extends PersistentTasksDecidersTestCase {

    public void testAllocationValues() {
        final String all = randomFrom("all", "All", "ALL");
        assertEquals(EnableAssignmentDecider.Allocation.ALL, EnableAssignmentDecider.Allocation.fromString(all));

        final String none = randomFrom("none", "None", "NONE");
        assertEquals(EnableAssignmentDecider.Allocation.NONE, EnableAssignmentDecider.Allocation.fromString(none));
    }

    public void testEnableAssignment() {
        final int nbTasks = randomIntBetween(1, 10);
        final int nbNodes = randomIntBetween(1, 5);
        final EnableAssignmentDecider.Allocation allocation = randomFrom(EnableAssignmentDecider.Allocation.values());

        Settings settings = Settings.builder()
            .put(EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.getKey(), allocation.toString())
            .build();
        updateSettings(settings);

        ClusterState clusterState = reassign(createClusterStateWithTasks(nbNodes, nbTasks));
        if (allocation == EnableAssignmentDecider.Allocation.ALL) {
            assertNbAssignedTasks(nbTasks, clusterState);
        } else {
            assertNbUnassignedTasks(nbTasks, clusterState);
        }
    }
}
