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

package org.opensearch.action.bulk;

import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.test.OpenSearchTestCase;

import static java.util.Collections.emptyMap;

public class TransportBulkActionHelperTests extends OpenSearchTestCase {

    public void testGetIndexWriteRequest() {
        IndexRequest indexRequest = new IndexRequest("index").id("id1").source(emptyMap());
        UpdateRequest upsertRequest = new UpdateRequest("index", "id1").upsert(indexRequest).script(mockScript("1"));
        UpdateRequest docAsUpsertRequest = new UpdateRequest("index", "id2").doc(indexRequest).docAsUpsert(true);
        UpdateRequest scriptedUpsert = new UpdateRequest("index", "id2").upsert(indexRequest).script(mockScript("1")).scriptedUpsert(true);

        assertEquals(TransportBulkActionHelper.getIndexWriteRequest(indexRequest), indexRequest);
        assertEquals(TransportBulkActionHelper.getIndexWriteRequest(upsertRequest), indexRequest);
        assertEquals(TransportBulkActionHelper.getIndexWriteRequest(docAsUpsertRequest), indexRequest);
        assertEquals(TransportBulkActionHelper.getIndexWriteRequest(scriptedUpsert), indexRequest);

        DeleteRequest deleteRequest = new DeleteRequest("index", "id");
        assertNull(TransportBulkActionHelper.getIndexWriteRequest(deleteRequest));

        UpdateRequest badUpsertRequest = new UpdateRequest("index", "id1");
        assertNull(TransportBulkActionHelper.getIndexWriteRequest(badUpsertRequest));
    }
}
