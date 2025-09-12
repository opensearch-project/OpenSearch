/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.suggest;

import org.opensearch.protobufs.Suggester;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class SuggestBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithEmptySuggester() {
        // Create an empty Suggester proto
        Suggester suggesterProto = Suggester.newBuilder().build();

        // Call the method under test
        SuggestBuilder suggestBuilder = SuggestBuilderProtoUtils.fromProto(suggesterProto);

        // Verify the result
        assertNotNull("SuggestBuilder should not be null", suggestBuilder);
    }
}
