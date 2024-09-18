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

package org.opensearch.search;

import org.apache.lucene.search.SortField;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.proto.search.SearchHitsProtoDef;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.transport.protobuf.SearchHitsProtobuf;

import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.sortFieldFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.sortFieldToProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.sortValueFromProto;
import static org.opensearch.transport.protobuf.ProtoSerDeHelpers.sortValueToProto;

public class SearchHitsProtobufTests extends AbstractWireSerializingTestCase<SearchHitsProtobuf> {

    public void testSortFieldProtoSerialization () {
        SortField[] fields = SearchHitsTests.createSortFields(randomIntBetween(1, 5));
        for (SortField orig : fields) {
            SearchHitsProtoDef.SortFieldProto proto = sortFieldToProto(orig);
            SortField cpy = sortFieldFromProto(proto);
            assertEquals(orig, cpy);
            assertEquals(orig.hashCode(), cpy.hashCode());
            assertNotSame(orig, cpy);
        }
    }

    public void testSortValueProtoSerialization () {
        Object[] values = SearchHitsTests.createCollapseValues(randomIntBetween(1, 10));
        for (Object orig : values) {
            SearchHitsProtoDef.SortValueProto proto = sortValueToProto(orig);
            Object cpy = sortValueFromProto(proto);
            assertEquals(orig, cpy);
            assertEquals(orig.hashCode(), cpy.hashCode());
        }
    }

    @Override
    protected Writeable.Reader<SearchHitsProtobuf> instanceReader() {
        return SearchHitsProtobuf::new;
    }

    @Override
    protected SearchHitsProtobuf createTestInstance() {
        return new SearchHitsProtobuf(SearchHitsTests.createTestItem(randomFrom(XContentType.values()), true, true));
    }

    @Override
    protected SearchHitsProtobuf mutateInstance(SearchHitsProtobuf instance) {
        return new SearchHitsProtobuf(SearchHitsTests.mutate(instance));
    }
}
