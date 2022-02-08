/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import static org.opensearch.index.query.IntervalsSourceProvider.Regexp;
import static org.opensearch.index.query.IntervalsSourceProvider.fromXContent;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RegexpIntervalsSourceProviderTests extends AbstractSerializingTestCase<Regexp> {
    private static final List<String> FLAGS = Arrays.asList("INTERSECTION", "COMPLEMENT", "EMPTY", "ANYSTRING", "INTERVAL", "NONE");

    @Override
    protected Regexp createTestInstance() {
        return createRandomRegexp();
    }

    static Regexp createRandomRegexp() {
        return new Regexp(
            randomAlphaOfLengthBetween(0, 3) + (randomBoolean() ? ".*?" : "." + randomAlphaOfLength(4)) + randomAlphaOfLengthBetween(0, 5),
            randomBoolean() ? RegexpFlag.resolveValue(randomFrom(FLAGS)) : RegexpFlag.ALL.value(),
            randomBoolean() ? randomAlphaOfLength(10) : null,
            randomBoolean() ? randomIntBetween(-1, Integer.MAX_VALUE) : null
        );
    }

    @Override
    protected Regexp mutateInstance(Regexp instance) throws IOException {
        String pattern = instance.getPattern();
        int flags = instance.getFlags();
        String useField = instance.getUseField();
        Integer maxExpansions = instance.getMaxExpansions();
        int ran = between(0, 3);
        switch (ran) {
            case 0:
                pattern += randomBoolean() ? ".*?" : randomAlphaOfLength(5);
                break;
            case 1:
                flags = (flags == RegexpFlag.ALL.value()) ? RegexpFlag.resolveValue(randomFrom(FLAGS)) : RegexpFlag.ALL.value();
                break;
            case 2:
                useField = useField == null ? randomAlphaOfLength(5) : null;
                break;
            case 3:
                maxExpansions = maxExpansions == null ? randomIntBetween(1, Integer.MAX_VALUE) : null;
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new Regexp(pattern, flags, useField, maxExpansions);
    }

    @Override
    protected Writeable.Reader<Regexp> instanceReader() {
        return Regexp::new;
    }

    @Override
    protected Regexp doParseInstance(XContentParser parser) throws IOException {
        if (parser.nextToken() == XContentParser.Token.START_OBJECT) {
            parser.nextToken();
        }
        Regexp regexp = (Regexp) fromXContent(parser);
        assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
        return regexp;
    }
}
