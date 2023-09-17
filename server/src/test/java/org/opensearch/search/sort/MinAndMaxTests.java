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

package org.opensearch.search.sort;

import org.apache.lucene.util.BytesRef;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigInteger;

public class MinAndMaxTests extends OpenSearchTestCase {

    public void testCompareMin() {
        assertEquals(true, new MinAndMax<Long>(0L, 9L).compareMin(15L) < 0); // LONG
        assertEquals(true, new MinAndMax<Integer>(0, 9).compareMin(15) < 0); // INT
        assertEquals(true, new MinAndMax<Float>(0f, 9f).compareMin(15f) < 0); // FLOAT
        assertEquals(true, new MinAndMax<Double>(0d, 9d).compareMin(15d) < 0); // DOUBLE
        assertEquals(true, new MinAndMax<BigInteger>(BigInteger.valueOf(0), BigInteger.valueOf(9)).compareMin(BigInteger.valueOf(15)) < 0); // BigInteger
        assertEquals(true, new MinAndMax<BytesRef>(new BytesRef("a"), new BytesRef("b")).compareMin(new BytesRef("c")) < 0); // ByteRef
        expectThrows(UnsupportedOperationException.class, () -> new MinAndMax<String>("a", "b").compareMin("c"));
    }

    public void testCompareMax() {
        assertEquals(true, new MinAndMax<Long>(0L, 9L).compareMax(15L) < 0); // LONG
        assertEquals(true, new MinAndMax<Integer>(0, 9).compareMax(15) < 0); // INT
        assertEquals(true, new MinAndMax<Float>(0f, 9f).compareMax(15f) < 0); // FLOAT
        assertEquals(true, new MinAndMax<Double>(0d, 9d).compareMax(15d) < 0); // DOUBLE
        assertEquals(true, new MinAndMax<BigInteger>(BigInteger.valueOf(0), BigInteger.valueOf(9)).compareMax(BigInteger.valueOf(15)) < 0); // BigInteger
        assertEquals(true, new MinAndMax<BytesRef>(new BytesRef("a"), new BytesRef("b")).compareMax(new BytesRef("c")) < 0); // ByteRef
        expectThrows(UnsupportedOperationException.class, () -> new MinAndMax<String>("a", "b").compareMax("c"));
    }
}
