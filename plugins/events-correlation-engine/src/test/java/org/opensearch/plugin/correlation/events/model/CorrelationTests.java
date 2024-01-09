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

public class CorrelationTests extends OpenSearchTestCase {

    public void testCorrelationAsStream() throws IOException {
        Correlation correlation = randomCorrelation(null, null, null, null, new float[] {}, null, null, null, null, null);
        BytesStreamOutput out = new BytesStreamOutput();
        correlation.writeTo(out);
        StreamInput sin = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        Correlation newCorrelation = Correlation.readFrom(sin);
        Assert.assertEquals("Round tripping Correlation doesn't work", correlation, newCorrelation);
    }

    public void testCorrelationParsing() throws IOException {
        Correlation correlation = randomCorrelation(null, null, null, null, new float[] {}, null, null, null, null, null);
        String correlationStr = TestHelpers.toJsonString(correlation);

        Correlation parsedCorrelation = Correlation.parse(TestHelpers.parse(correlationStr));
        parsedCorrelation.setId("");
        parsedCorrelation.setVersion(1L);

        Assert.assertEquals(correlation, parsedCorrelation);
    }

    private Correlation randomCorrelation(
        Boolean isRootParam,
        Long levelParam,
        String event1Param,
        String event2Param,
        float[] correlationVectorParam,
        Long timestampParam,
        String index1Param,
        String index2Param,
        List<String> tagsParam,
        Long scoreTimestampParam
    ) {
        Boolean isRoot = isRootParam != null ? isRootParam : false;
        Long level = levelParam != null ? levelParam : 1L;
        String event1 = event1Param != null ? event1Param : RandomStrings.randomAsciiLettersOfLength(Randomness.get(), 10);
        String event2 = event2Param != null ? event2Param : RandomStrings.randomAsciiLettersOfLength(Randomness.get(), 10);
        float[] correlationVector = correlationVectorParam.length > 0 ? correlationVectorParam : new float[] { 1.0f, 1.0f, 1.0f };
        Long timestamp = timestampParam != null ? timestampParam : System.currentTimeMillis();
        String index1 = index1Param != null ? index1Param : RandomStrings.randomAsciiLettersOfLength(Randomness.get(), 10);
        String index2 = index2Param != null ? index2Param : RandomStrings.randomAsciiLettersOfLength(Randomness.get(), 10);
        List<String> tags = tagsParam != null ? tagsParam : List.of(RandomStrings.randomAsciiLettersOfLength(Randomness.get(), 10));
        Long scoreTimestamp = scoreTimestampParam != null ? scoreTimestampParam : System.currentTimeMillis();
        return new Correlation("", 1L, isRoot, level, event1, event2, correlationVector, timestamp, index1, index2, tags, scoreTimestamp);
    }
}
