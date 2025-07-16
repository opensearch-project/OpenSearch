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

package org.opensearch.index.query;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.util.CollectionUtils;

import java.io.IOException;
import java.util.Locale;

/**
 * Boolean operators
 *
 * @opensearch.internal
 */
public enum Operator implements Writeable {
    OR,
    AND;

    public BooleanClause.Occur toBooleanClauseOccur() {
        switch (this) {
            case OR:
                return BooleanClause.Occur.SHOULD;
            case AND:
                return BooleanClause.Occur.MUST;
            default:
                throw Operator.newOperatorException(this.toString());
        }
    }

    public QueryParser.Operator toQueryParserOperator() {
        switch (this) {
            case OR:
                return QueryParser.Operator.OR;
            case AND:
                return QueryParser.Operator.AND;
            default:
                throw Operator.newOperatorException(this.toString());
        }
    }

    public static Operator readFromStream(StreamInput in) throws IOException {
        return in.readEnum(Operator.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static Operator fromString(String op) {
        return valueOf(op.toUpperCase(Locale.ROOT));
    }

    public static IllegalArgumentException newOperatorException(String op) {
        return new IllegalArgumentException(
            "operator needs to be either " + CollectionUtils.arrayAsArrayList(values()) + ", but not [" + op + "]"
        );
    }
}
