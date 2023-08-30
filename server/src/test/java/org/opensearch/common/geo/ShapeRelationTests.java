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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.common.geo;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ShapeRelationTests extends OpenSearchTestCase {

    public void testValidOrdinals() {
        assertThat(ShapeRelation.INTERSECTS.ordinal(), equalTo(0));
        assertThat(ShapeRelation.DISJOINT.ordinal(), equalTo(1));
        assertThat(ShapeRelation.WITHIN.ordinal(), equalTo(2));
    }

    public void testwriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ShapeRelation.INTERSECTS.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ShapeRelation.DISJOINT.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            ShapeRelation.WITHIN.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(2));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(ShapeRelation.readFromStream(in), equalTo(ShapeRelation.INTERSECTS));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(ShapeRelation.readFromStream(in), equalTo(ShapeRelation.DISJOINT));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(2);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(ShapeRelation.readFromStream(in), equalTo(ShapeRelation.WITHIN));
            }
        }
    }

    public void testInvalidReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(randomIntBetween(3, Integer.MAX_VALUE));
            try (StreamInput in = out.bytes().streamInput()) {
                ShapeRelation.readFromStream(in);
                fail("Expected IOException");
            } catch (IOException e) {
                assertThat(e.getMessage(), containsString("Unknown ShapeRelation ordinal ["));
            }

        }
    }
}
