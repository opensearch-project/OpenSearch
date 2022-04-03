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

package org.opensearch.search.aggregations.pipeline;

import org.opensearch.common.ParsingException;
import org.opensearch.common.io.stream.AbstractWriteableEnumTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class GapPolicyTests extends AbstractWriteableEnumTestCase {

    public GapPolicyTests() {
        super(BucketHelpers.GapPolicy::readFrom);
    }

    @Override
    public void testValidOrdinals() {
        assertThat(BucketHelpers.GapPolicy.INSERT_ZEROS.ordinal(), equalTo(0));
        assertThat(BucketHelpers.GapPolicy.SKIP.ordinal(), equalTo(1));
    }

    @Override
    public void testFromString() {
        assertThat(BucketHelpers.GapPolicy.parse("insert_zeros", null), equalTo(BucketHelpers.GapPolicy.INSERT_ZEROS));
        assertThat(BucketHelpers.GapPolicy.parse("skip", null), equalTo(BucketHelpers.GapPolicy.SKIP));
        ParsingException e = expectThrows(ParsingException.class, () -> BucketHelpers.GapPolicy.parse("does_not_exist", null));
        assertThat(e.getMessage(), equalTo("Invalid gap policy: [does_not_exist], accepted values: [insert_zeros, skip]"));
    }

    @Override
    public void testReadFrom() throws IOException {
        assertReadFromStream(0, BucketHelpers.GapPolicy.INSERT_ZEROS);
        assertReadFromStream(1, BucketHelpers.GapPolicy.SKIP);
    }

    @Override
    public void testWriteTo() throws IOException {
        assertWriteToStream(BucketHelpers.GapPolicy.INSERT_ZEROS, 0);
        assertWriteToStream(BucketHelpers.GapPolicy.SKIP, 1);
    }
}
