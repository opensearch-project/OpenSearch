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

package org.opensearch.search.suggest.phrase;

import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.text.Text;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.search.suggest.Suggest.Suggestion;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.core.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Suggestion entry returned from the {@link PhraseSuggester}.
 *
 * @opensearch.internal
 */
public class PhraseSuggestion extends Suggest.Suggestion<PhraseSuggestion.Entry> {

    @Deprecated
    public static final int TYPE = 3;

    public PhraseSuggestion(String name, int size) {
        super(name, size);
    }

    public PhraseSuggestion(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return PhraseSuggestionBuilder.SUGGESTION_NAME;
    }

    @Override
    public int getWriteableType() {
        return TYPE;
    }

    @Override
    protected Entry newEntry(StreamInput in) throws IOException {
        return new Entry(in);
    }

    public static PhraseSuggestion fromXContent(XContentParser parser, String name) throws IOException {
        PhraseSuggestion suggestion = new PhraseSuggestion(name, -1);
        parseEntries(parser, suggestion, PhraseSuggestion.Entry::fromXContent);
        return suggestion;
    }

    /**
     * Entry in the phrase suggester
     *
     * @opensearch.internal
     */
    public static class Entry extends Suggestion.Entry<PhraseSuggestion.Entry.Option> {

        protected double cutoffScore = Double.MIN_VALUE;

        public Entry(Text text, int offset, int length, double cutoffScore) {
            super(text, offset, length);
            this.cutoffScore = cutoffScore;
        }

        public Entry(Text text, int offset, int length) {
            super(text, offset, length);
        }

        Entry() {}

        public Entry(StreamInput in) throws IOException {
            super(in);
            cutoffScore = in.readDouble();
        }

        /**
         * @return cutoff score for suggestions.  input term score * confidence for phrase suggest, 0 otherwise
         */
        public double getCutoffScore() {
            return cutoffScore;
        }

        @Override
        protected void merge(Suggestion.Entry<Option> other) {
            super.merge(other);
            // If the cluster contains both pre 0.90.4 and post 0.90.4 nodes then we'll see Suggestion.Entry
            // objects being merged with PhraseSuggestion.Entry objects. We merge Suggestion.Entry objects
            // by assuming they had a low cutoff score rather than a high one as that is the more common scenario
            // and the simplest one for us to implement.
            if (!(other instanceof PhraseSuggestion.Entry otherSuggestionEntry)) {
                return;
            }
            this.cutoffScore = Math.max(this.cutoffScore, otherSuggestionEntry.cutoffScore);
        }

        @Override
        public void addOption(Option option) {
            if (option.getScore() > this.cutoffScore) {
                this.options.add(option);
            }
        }

        private static final ObjectParser<Entry, Void> PARSER = new ObjectParser<>("PhraseSuggestionEntryParser", true, Entry::new);
        static {
            declareCommonFields(PARSER);
            /*
             * The use of a lambda expression instead of the method reference Entry::addOptions is a workaround for a JDK 14 compiler bug.
             * The bug is: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8242214
             */
            PARSER.declareObjectArray((e, o) -> e.addOptions(o), (p, c) -> Option.fromXContent(p), new ParseField(OPTIONS));
        }

        public static Entry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        protected Option newOption(StreamInput in) throws IOException {
            return new Option(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeDouble(cutoffScore);
        }

        @Override
        public boolean equals(Object other) {
            return super.equals(other) && Objects.equals(cutoffScore, ((Entry) other).cutoffScore);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), cutoffScore);
        }

        /**
         * Options for phrase suggestion
         *
         * @opensearch.internal
         */
        public static class Option extends Suggestion.Entry.Option {

            public Option(Text text, Text highlighted, float score, Boolean collateMatch) {
                super(text, highlighted, score, collateMatch);
            }

            public Option(Text text, Text highlighted, float score) {
                super(text, highlighted, score);
            }

            public Option(StreamInput in) throws IOException {
                super(in);
            }

            private static final ConstructingObjectParser<Option, Void> PARSER = new ConstructingObjectParser<>(
                "PhraseOptionParser",
                true,
                args -> {
                    Text text = new Text((String) args[0]);
                    float score = (Float) args[1];
                    String highlighted = (String) args[2];
                    Text highlightedText = highlighted == null ? null : new Text(highlighted);
                    Boolean collateMatch = (Boolean) args[3];
                    return new Option(text, highlightedText, score, collateMatch);
                }
            );

            static {
                PARSER.declareString(constructorArg(), TEXT);
                PARSER.declareFloat(constructorArg(), SCORE);
                PARSER.declareString(optionalConstructorArg(), HIGHLIGHTED);
                PARSER.declareBoolean(optionalConstructorArg(), COLLATE_MATCH);
            }

            public static Option fromXContent(XContentParser parser) {
                return PARSER.apply(parser, null);
            }
        }
    }
}
