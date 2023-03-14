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

package org.opensearch.example.customsuggester;

import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.text.Text;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ObjectParser;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.suggest.Suggest;

import java.io.IOException;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * The suggestion responses corresponding with the suggestions in the request.
 */
public class CustomSuggestion extends Suggest.Suggestion<CustomSuggestion.Entry> {

    /**
     * An integer representing the type of the suggestion formerly used for internal serialization over the network.
     *
     * This class is now serialized as a NamedWriteable and this value only remains for backwards compatibility
     */
    public static final int TYPE = 999;

    /**
     * A meaningless value used to test that plugin suggesters can add fields to their Suggestion types.
     */
    public static final ParseField DUMMY = new ParseField("dummy");

    private String dummy;

    /**
     * Instantiate this object with the specified name, size, and value for the configured field.
     *
     * @param name The name of the suggestion as is defined in the request.
     * @param size The suggested term size specified in request, only used for merging shard responses.
     * @param dummy The added custom suggestion type.
     */
    public CustomSuggestion(String name, int size, String dummy) {
        super(name, size);
        this.dummy = dummy;
    }

    /**
     * Instantiate this object from a stream.
     *
     * @param in Input to read the value from
     * @throws IOException on failure to read the value.
     */
    public CustomSuggestion(StreamInput in) throws IOException {
        super(in);
        dummy = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(dummy);
    }

    @Override
    public String getWriteableName() {
        return CustomSuggestionBuilder.SUGGESTION_NAME;
    }

    @Override
    public int getWriteableType() {
        return TYPE;
    }

    /**
     * A meaningless value used to test that plugin suggesters can add fields to their Suggestion types
     *
     * This can't be serialized to xcontent because Suggestions appear in xcontent as an array of entries, so there is no place
     * to add a custom field. But we can still use a custom field internally and use it to define a Suggestion's behavior
     *
     * @return the value.
     */
    public String getDummy() {
        return dummy;
    }

    @Override
    protected Entry newEntry(StreamInput in) throws IOException {
        return new Entry(in);
    }

    /**
     * Instantiate a CustomSuggestion from XContent.
     *
     * @param parser The XContent parser to use
     * @param name Tne name of the suggestion
     * @return A new CustomSuggestion instance for the specified name.
     * @throws IOException on deserialization error.
     */
    public static CustomSuggestion fromXContent(XContentParser parser, String name) throws IOException {
        CustomSuggestion suggestion = new CustomSuggestion(name, -1, null);
        parseEntries(parser, suggestion, Entry::fromXContent);
        return suggestion;
    }

    /**
     * Represents a part from the suggest text with suggested options.
     */
    public static class Entry extends Suggest.Suggestion.Entry<CustomSuggestion.Entry.Option> {

        private static final ObjectParser<Entry, Void> PARSER = new ObjectParser<>("CustomSuggestionEntryParser", true, Entry::new);

        static {
            declareCommonFields(PARSER);
            PARSER.declareString((entry, dummy) -> entry.dummy = dummy, DUMMY);
            /*
             * The use of a lambda expression instead of the method reference Entry::addOptions is a workaround for a JDK 14 compiler bug.
             * The bug is: https://bugs.java.com/bugdatabase/view_bug.do?bug_id=JDK-8242214
             */
            PARSER.declareObjectArray((e, o) -> e.addOptions(o), (p, c) -> Option.fromXContent(p), new ParseField(OPTIONS));
        }

        private String dummy;

        /**
         * Instantiate this object.
         */
        public Entry() {}

        /**
         * Instantiate this object from the given text, offset, length, and additional dummy field.
         *
         * @param text the text originating from the suggest text.
         * @param offset the start offset for this entry in the suggest text.
         * @param length the length for this entry in the suggest text.
         * @param dummy the additional custom field.
         */
        public Entry(Text text, int offset, int length, String dummy) {
            super(text, offset, length);
            this.dummy = dummy;
        }

        /**
         * Instantiate this object from a stream.
         *
         * @param in Input to read the value from
         * @throws IOException on failure to read the value.
         */
        public Entry(StreamInput in) throws IOException {
            super(in);
            dummy = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(dummy);
        }

        @Override
        protected Option newOption(StreamInput in) throws IOException {
            return new Option(in);
        }

        /*
         * the value of dummy will always be the same, so this just tests that we can merge entries with custom fields
         *
         * @InheritDoc
         */
        @Override
        protected void merge(Suggest.Suggestion.Entry<Option> otherEntry) {
            dummy = ((Entry) otherEntry).getDummy();
        }

        /**
         * Meaningless field used to test that plugin suggesters can add fields to their entries
         *
         * @return the dummy field value
         */
        public String getDummy() {
            return dummy;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder = super.toXContent(builder, params);
            builder.field(DUMMY.getPreferredName(), getDummy());
            return builder;
        }

        /**
         * Create a Suggestion Entry from XContent
         *
         * @param parser The parser to use
         * @return the Suggestion Entry
         */
        public static Entry fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        /**
         * An Entry Option to use with a Suggestion.
         */
        public static class Option extends Suggest.Suggestion.Entry.Option {

            private static final ConstructingObjectParser<Option, Void> PARSER = new ConstructingObjectParser<>(
                "CustomSuggestionObjectParser",
                true,
                args -> {
                    Text text = new Text((String) args[0]);
                    float score = (float) args[1];
                    String dummy = (String) args[2];
                    return new Option(text, score, dummy);
                }
            );

            static {
                PARSER.declareString(constructorArg(), TEXT);
                PARSER.declareFloat(constructorArg(), SCORE);
                PARSER.declareString(constructorArg(), DUMMY);
            }

            private String dummy;

            /**
             * Instantiate this object from the specified text, score, and dummy field
             *
             * @param text The option text.
             * @param score The option score.
             * @param dummy The value of the dummy field.
             */
            public Option(Text text, float score, String dummy) {
                super(text, score);
                this.dummy = dummy;
            }

            /**
             * Instantiate this object from a stream.
             *
             * @param in Input to read the value from
             * @throws IOException on failure to read the value.
             */
            public Option(StreamInput in) throws IOException {
                super(in);
                dummy = in.readString();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                super.writeTo(out);
                out.writeString(dummy);
            }

            /**
             * A meaningless value used to test that plugin suggesters can add fields to their options
             */
            public String getDummy() {
                return dummy;
            }

            /*
             * the value of dummy will always be the same, so this just tests that we can merge options with custom fields
             */
            @Override
            protected void mergeInto(Suggest.Suggestion.Entry.Option otherOption) {
                super.mergeInto(otherOption);
                dummy = ((Option) otherOption).getDummy();
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder = super.toXContent(builder, params);
                builder.field(DUMMY.getPreferredName(), dummy);
                return builder;
            }

            /**
             * Instantiate an Option from XContent.
             *
             * @param parser The XContent parser to use
             */
            public static Option fromXContent(XContentParser parser) {
                return PARSER.apply(parser, null);
            }
        }
    }
}
