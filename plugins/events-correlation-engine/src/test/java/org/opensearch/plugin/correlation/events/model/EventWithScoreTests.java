/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.correlation.events.model;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.opensearch.common.Randomness;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.correlation.utils.TestHelpers;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.io.IOException;
import java.util.List;

public class EventWithScoreTests extends OpenSearchTestCase {

    public void testEventWithScoreAsStream() throws IOException {
        EventWithScore eventWithScore = randomEventWithScore(null, null, null, null);
        BytesStreamOutput out = new BytesStreamOutput();
        eventWithScore.writeTo(out);
        StreamInput sin = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        EventWithScore newEventWithScore = EventWithScore.readFrom(sin);
        Assert.assertEquals("Round tripping EventWithScore doesn't work", eventWithScore, newEventWithScore);
    }

    public void testEventWithScoreParsing() throws IOException {
        EventWithScore eventWithScore = randomEventWithScore(null, null, null, null);
        String eventWithScoreStr = TestHelpers.toJsonString(eventWithScore);

        EventWithScore parsedEventWithScore = EventWithScore.parse(TestHelpers.parse(eventWithScoreStr));
        Assert.assertEquals(eventWithScore, parsedEventWithScore);
    }

    private EventWithScore randomEventWithScore(String indexParam, String eventParam, Double scoreParam, List<String> tagsParam) {
        String index = indexParam != null ? indexParam : RandomStrings.randomAsciiLettersOfLength(Randomness.get(), 10);
        String event = eventParam != null ? eventParam : RandomStrings.randomAsciiLettersOfLength(Randomness.get(), 10);
        Double score = scoreParam != null ? scoreParam : 0.0;
        List<String> tags = tagsParam != null ? tagsParam : List.of();
        return new EventWithScore(index, event, score, tags);
    }
}
