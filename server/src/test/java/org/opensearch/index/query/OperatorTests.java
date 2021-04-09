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

package org.opensearch.index.query;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.equalTo;

public class OperatorTests extends OpenSearchTestCase {

    public void testValidOrdinals() {
        assertThat(Operator.OR.ordinal(), equalTo(0));
        assertThat(Operator.AND.ordinal(), equalTo(1));
    }

    public void testWriteTo() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.OR.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(0));
            }
        }

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            Operator.AND.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(in.readVInt(), equalTo(1));
            }
        }
    }

    public void testReadFrom() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(0);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.OR));
            }
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(1);
            try (StreamInput in = out.bytes().streamInput()) {
                assertThat(Operator.readFromStream(in), equalTo(Operator.AND));
            }
        }
    }

    public void testToBooleanClauseOccur() {
        assertThat(Operator.AND.toBooleanClauseOccur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(Operator.OR.toBooleanClauseOccur(), equalTo(BooleanClause.Occur.SHOULD));
    }

    public void testToQueryParserOperator() {
        assertThat(Operator.AND.toQueryParserOperator(), equalTo(QueryParser.Operator.AND));
        assertThat(Operator.OR.toQueryParserOperator(), equalTo(QueryParser.Operator.OR));
    }

    public void testFromString() {
        assertThat(Operator.fromString("and"), equalTo(Operator.AND));
        assertThat(Operator.fromString("AND"), equalTo(Operator.AND));
        assertThat(Operator.fromString("AnD"), equalTo(Operator.AND));
        assertThat(Operator.fromString("or"), equalTo(Operator.OR));
        assertThat(Operator.fromString("OR"), equalTo(Operator.OR));
        assertThat(Operator.fromString("Or"), equalTo(Operator.OR));
    }
}
