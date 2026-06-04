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

package org.opensearch.painless;

public class CommentTests extends ScriptTestCase {

    public void testSingleLineComments() {
        assertEquals(5, exec("// comment\n return 5"));
        assertEquals(5, exec("// comment\r return 5"));
        assertEquals(5, exec("return 5 // comment no newline or return char"));
    }

    public void testOpenCloseComments() {
        assertEquals(5, exec("/* single-line comment */ return 5"));
        assertEquals(5, exec("/* multi-line \n */ return 5"));
        assertEquals(5, exec("/* multi-line \r */ return 5"));
        assertEquals(5, exec("/* multi-line \n\n\r\r */ return 5"));
        assertEquals(5, exec("def five = 5; /* multi-line \r */ return five"));
        assertEquals(5, exec("return 5 /* multi-line ignored code */"));
    }
}
