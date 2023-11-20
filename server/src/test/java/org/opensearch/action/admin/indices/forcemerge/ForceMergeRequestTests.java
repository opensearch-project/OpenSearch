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

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.test.OpenSearchTestCase;

public class ForceMergeRequestTests extends OpenSearchTestCase {

    public void testDescription() {
        ForceMergeRequest request = new ForceMergeRequest();
        assertEquals(
            "Force-merge indices [], maxSegments[-1], onlyExpungeDeletes[false], flush[true], primaryOnly[false]",
            request.getDescription()
        );

        request = new ForceMergeRequest("shop", "blog");
        assertEquals(
            "Force-merge indices [shop, blog], maxSegments[-1], onlyExpungeDeletes[false], flush[true], primaryOnly[false]",
            request.getDescription()
        );

        request = new ForceMergeRequest();
        request.maxNumSegments(12);
        request.onlyExpungeDeletes(true);
        request.flush(false);
        request.primaryOnly(true);
        assertEquals(
            "Force-merge indices [], maxSegments[12], onlyExpungeDeletes[true], flush[false], primaryOnly[true]",
            request.getDescription()
        );
    }

    public void testToString() {
        ForceMergeRequest request = new ForceMergeRequest();
        assertEquals("ForceMergeRequest{maxNumSegments=-1, onlyExpungeDeletes=false, flush=true, primaryOnly=false}", request.toString());

        request = new ForceMergeRequest();
        request.maxNumSegments(12);
        request.onlyExpungeDeletes(true);
        request.flush(false);
        request.primaryOnly(true);
        assertEquals("ForceMergeRequest{maxNumSegments=12, onlyExpungeDeletes=true, flush=false, primaryOnly=true}", request.toString());
    }
}
