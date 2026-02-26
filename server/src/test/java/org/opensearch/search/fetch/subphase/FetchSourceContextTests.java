/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.fetch.subphase;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.memory.MemoryIndex;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchHit;
import org.opensearch.search.fetch.FetchContext;
import org.opensearch.search.fetch.FetchSubPhase.HitContext;
import org.opensearch.search.fetch.FetchSubPhaseProcessor;
import org.opensearch.search.lookup.SourceLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

// ISSUE-20612 Source Validation
public class FetchSourceContextTests extends OpenSearchTestCase {

    private XContentParser createSourceParser(XContentBuilder source) throws IOException {
        XContentParser parser = createParser(source);
        parser.nextToken(); // move to start object
        parser.nextToken(); // move to field name "_source"
        parser.nextToken(); // move to _source value to parse
        return parser;
    }

    public void testFetchSource() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject().field("_source", true).endObject();
        XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertEquals(FetchSourceContext.FETCH_SOURCE, result);
    }

    public void testDoNotFetchSource() throws IOException {
        XContentBuilder source = XContentFactory.jsonBuilder()
            .startObject().field("_source", false).endObject();
        XContentParser parser = createSourceParser(source);

        FetchSourceContext result = FetchSourceContext.fromXContent(parser);
        assertEquals(FetchSourceContext.DO_NOT_FETCH_SOURCE, result);
    }
}
