/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.opensearch.common.blobstore;

import org.opensearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class BlobPathTests extends ESTestCase {

    public void testBuildAsString() {
        BlobPath path = new BlobPath();
        assertThat(path.buildAsString(), is(""));

        path = path.add("a");
        assertThat(path.buildAsString(), is("a/"));

        path = path.add("b").add("c");
        assertThat(path.buildAsString(), is("a/b/c/"));

        path = path.add("d/");
        assertThat(path.buildAsString(), is("a/b/c/d/"));
    }
}
