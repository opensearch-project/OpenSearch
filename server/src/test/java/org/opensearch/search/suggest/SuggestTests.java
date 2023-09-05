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

package org.opensearch.search.suggest;

import org.opensearch.Version;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.search.SearchModule;
import org.opensearch.search.suggest.Suggest.Suggestion;
import org.opensearch.search.suggest.Suggest.Suggestion.Entry;
import org.opensearch.search.suggest.Suggest.Suggestion.Entry.Option;
import org.opensearch.search.suggest.completion.CompletionSuggestion;
import org.opensearch.search.suggest.phrase.PhraseSuggestion;
import org.opensearch.search.suggest.term.TermSuggestion;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.opensearch.common.xcontent.XContentHelper.stripWhitespace;
import static org.opensearch.common.xcontent.XContentHelper.toXContent;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureFieldName;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;

public class SuggestTests extends OpenSearchTestCase {

    private static final NamedXContentRegistry xContentRegistry;
    private static final List<NamedXContentRegistry.Entry> namedXContents;

    static {
        namedXContents = new ArrayList<>();
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                Suggest.Suggestion.class,
                new ParseField("term"),
                (parser, context) -> TermSuggestion.fromXContent(parser, (String) context)
            )
        );
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                Suggest.Suggestion.class,
                new ParseField("phrase"),
                (parser, context) -> PhraseSuggestion.fromXContent(parser, (String) context)
            )
        );
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                Suggest.Suggestion.class,
                new ParseField("completion"),
                (parser, context) -> CompletionSuggestion.fromXContent(parser, (String) context)
            )
        );
        xContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    public static List<NamedXContentRegistry.Entry> getDefaultNamedXContents() {
        return namedXContents;
    }

    static NamedXContentRegistry getSuggestersRegistry() {
        return xContentRegistry;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return getSuggestersRegistry();
    }

    public static Suggest createTestItem() {
        int numEntries = randomIntBetween(0, 5);
        List<Suggestion<? extends Entry<? extends Option>>> suggestions = new ArrayList<>();
        for (int i = 0; i < numEntries; i++) {
            suggestions.add(SuggestionTests.createTestItem());
        }
        return new Suggest(suggestions);
    }

    public void testFromXContent() throws IOException {
        ToXContent.Params params = new ToXContent.MapParams(Collections.singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        Suggest suggest = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(suggest, xContentType, params, humanReadable);
        Suggest parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), Suggest.NAME);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = Suggest.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertEquals(suggest.size(), parsed.size());
        for (Suggestion suggestion : suggest) {
            Suggestion<? extends Entry<? extends Option>> parsedSuggestion = parsed.getSuggestion(suggestion.getName());
            assertNotNull(parsedSuggestion);
            assertEquals(suggestion.getClass(), parsedSuggestion.getClass());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, params, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        PhraseSuggestion.Entry.Option option = new PhraseSuggestion.Entry.Option(
            new Text("someText"),
            new Text("somethingHighlighted"),
            1.3f,
            true
        );
        PhraseSuggestion.Entry entry = new PhraseSuggestion.Entry(new Text("entryText"), 42, 313);
        entry.addOption(option);
        PhraseSuggestion suggestion = new PhraseSuggestion("suggestionName", 5);
        suggestion.addTerm(entry);
        Suggest suggest = new Suggest(Collections.singletonList(suggestion));
        BytesReference xContent = toXContent(suggest, XContentType.JSON, randomBoolean());
        assertEquals(
            stripWhitespace(
                "{"
                    + "  \"suggest\": {"
                    + "    \"suggestionName\": ["
                    + "      {"
                    + "        \"text\": \"entryText\","
                    + "        \"offset\": 42,"
                    + "        \"length\": 313,"
                    + "        \"options\": ["
                    + "          {"
                    + "            \"text\": \"someText\","
                    + "            \"highlighted\": \"somethingHighlighted\","
                    + "            \"score\": 1.3,"
                    + "            \"collate_match\": true"
                    + "          }"
                    + "        ]"
                    + "      }"
                    + "    ]"
                    + "  }"
                    + "}"
            ),
            xContent.utf8ToString()
        );
    }

    public void testFilter() throws Exception {
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions;
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(randomAlphaOfLength(10), 2, false);
        PhraseSuggestion phraseSuggestion = new PhraseSuggestion(randomAlphaOfLength(10), 2);
        TermSuggestion termSuggestion = new TermSuggestion(randomAlphaOfLength(10), 2, SortBy.SCORE);
        suggestions = Arrays.asList(completionSuggestion, phraseSuggestion, termSuggestion);
        Suggest suggest = new Suggest(suggestions);
        List<PhraseSuggestion> phraseSuggestions = suggest.filter(PhraseSuggestion.class);
        assertThat(phraseSuggestions.size(), equalTo(1));
        assertThat(phraseSuggestions.get(0), equalTo(phraseSuggestion));
        List<TermSuggestion> termSuggestions = suggest.filter(TermSuggestion.class);
        assertThat(termSuggestions.size(), equalTo(1));
        assertThat(termSuggestions.get(0), equalTo(termSuggestion));
        List<CompletionSuggestion> completionSuggestions = suggest.filter(CompletionSuggestion.class);
        assertThat(completionSuggestions.size(), equalTo(1));
        assertThat(completionSuggestions.get(0), equalTo(completionSuggestion));
    }

    public void testSuggestionOrdering() throws Exception {
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions;
        suggestions = new ArrayList<>();
        int n = randomIntBetween(2, 5);
        for (int i = 0; i < n; i++) {
            suggestions.add(new CompletionSuggestion(randomAlphaOfLength(10), randomIntBetween(3, 5), false));
        }
        Collections.shuffle(suggestions, random());
        Suggest suggest = new Suggest(suggestions);
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> sortedSuggestions;
        sortedSuggestions = new ArrayList<>(suggestions);
        sortedSuggestions.sort((o1, o2) -> o1.getName().compareTo(o2.getName()));
        List<CompletionSuggestion> completionSuggestions = suggest.filter(CompletionSuggestion.class);
        assertThat(completionSuggestions.size(), equalTo(n));
        for (int i = 0; i < n; i++) {
            assertThat(completionSuggestions.get(i).getName(), equalTo(sortedSuggestions.get(i).getName()));
        }
    }

    public void testParsingExceptionOnUnknownSuggestion() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.startArray("unknownSuggestion");
            builder.endArray();
        }
        builder.endObject();
        BytesReference originalBytes = BytesReference.bytes(builder);
        try (XContentParser parser = createParser(builder.contentType().xContent(), originalBytes)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            ParsingException ex = expectThrows(ParsingException.class, () -> Suggest.fromXContent(parser));
            assertEquals("Could not parse suggestion keyed as [unknownSuggestion]", ex.getMessage());
        }
    }

    public void testMergingSuggestionOptions() {
        String suggestedWord = randomAlphaOfLength(10);
        String secondWord = randomAlphaOfLength(10);
        Text suggestionText = new Text(suggestedWord + " " + secondWord);
        Text highlighted = new Text("<em>" + suggestedWord + "</em> " + secondWord);
        PhraseSuggestion.Entry.Option option1 = new PhraseSuggestion.Entry.Option(suggestionText, highlighted, 0.7f, false);
        PhraseSuggestion.Entry.Option option2 = new PhraseSuggestion.Entry.Option(suggestionText, highlighted, 0.8f, true);
        PhraseSuggestion.Entry.Option option3 = new PhraseSuggestion.Entry.Option(suggestionText, highlighted, 0.6f);
        assertEquals(suggestionText, option1.getText());
        assertEquals(highlighted, option1.getHighlighted());
        assertFalse(option1.collateMatch());
        assertTrue(option1.getScore() > 0.6f);
        option1.mergeInto(option2);
        assertEquals(suggestionText, option1.getText());
        assertEquals(highlighted, option1.getHighlighted());
        assertTrue(option1.collateMatch());
        assertTrue(option1.getScore() > 0.7f);
        option1.mergeInto(option3);
        assertEquals(suggestionText, option1.getText());
        assertEquals(highlighted, option1.getHighlighted());
        assertTrue(option1.getScore() > 0.7f);
        assertTrue(option1.collateMatch());
    }

    public void testSerialization() throws IOException {
        final Version bwcVersion = VersionUtils.randomVersionBetween(
            random(),
            Version.CURRENT.minimumCompatibilityVersion(),
            Version.CURRENT
        );

        final Suggest suggest = createTestItem();
        final Suggest bwcSuggest;

        NamedWriteableRegistry registry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, emptyList()).getNamedWriteables());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(bwcVersion);
            suggest.writeTo(out);
            try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                in.setVersion(bwcVersion);
                bwcSuggest = new Suggest(in);
            }
        }

        assertEquals(suggest, bwcSuggest);

        final Suggest backAgain;

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.CURRENT);
            bwcSuggest.writeTo(out);
            try (NamedWriteableAwareStreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                in.setVersion(Version.CURRENT);
                backAgain = new Suggest(in);
            }
        }

        assertEquals(suggest, backAgain);
    }
}
