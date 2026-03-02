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

package org.opensearch.search.fetch.subphase;

import org.opensearch.OpenSearchException;
import org.opensearch.common.Booleans;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.RestRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Context used to fetch the {@code _source}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class FetchSourceContext implements Writeable, ToXContentObject {

    public static final ParseField INCLUDES_FIELD = new ParseField("includes", "include");
    public static final ParseField EXCLUDES_FIELD = new ParseField("excludes", "exclude");

    public static final FetchSourceContext FETCH_SOURCE = new FetchSourceContext(true);
    public static final FetchSourceContext DO_NOT_FETCH_SOURCE = new FetchSourceContext(false);

    private static final String AMBIGUOUS_FIELD_MESSAGE = "The same entry [{}] cannot be both included and excluded in _source.";

    private final boolean fetchSource;
    private final String[] includes;
    private final String[] excludes;
    private Function<Map<String, ?>, Map<String, Object>> filter;

    public FetchSourceContext(boolean fetchSource, String[] includes, String[] excludes) {
        this.fetchSource = fetchSource;
        this.includes = includes == null ? Strings.EMPTY_ARRAY : includes;
        this.excludes = excludes == null ? Strings.EMPTY_ARRAY : excludes;
        validateAmbiguousFields();
    }

    public FetchSourceContext(boolean fetchSource) {
        this(fetchSource, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
    }

    public FetchSourceContext(StreamInput in) throws IOException {
        fetchSource = in.readBoolean();
        includes = in.readStringArray();
        excludes = in.readStringArray();
    }

    /**
     * The same entry cannot be both included and excluded in _source.
     * Since the constructors are public, this validation is required to be called in the constructor.
     * */
    private void validateAmbiguousFields() {
        Set<String> includeSet = new HashSet<>(Arrays.asList(this.includes));
        for (String exclude : this.excludes) {
            if (includeSet.contains(exclude)) {
                throw new OpenSearchException(AMBIGUOUS_FIELD_MESSAGE, exclude);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(fetchSource);
        out.writeStringArray(includes);
        out.writeStringArray(excludes);
    }

    public boolean fetchSource() {
        return this.fetchSource;
    }

    public String[] includes() {
        return this.includes;
    }

    public String[] excludes() {
        return this.excludes;
    }

    public static FetchSourceContext parseFromRestRequest(RestRequest request) {
        Boolean fetchSource = null;
        String[] sourceExcludes = null;
        String[] sourceIncludes = null;

        String source = request.param("_source");
        if (source != null) {
            if (Booleans.isTrue(source)) {
                fetchSource = true;
            } else if (Booleans.isFalse(source)) {
                fetchSource = false;
            } else {
                sourceIncludes = Strings.splitStringByCommaToArray(source);
            }
        }

        String sIncludes = request.param("_source_includes");
        if (sIncludes != null) {
            sourceIncludes = Strings.splitStringByCommaToArray(sIncludes);
        }

        String sExcludes = request.param("_source_excludes");
        if (sExcludes != null) {
            sourceExcludes = Strings.splitStringByCommaToArray(sExcludes);
        }

        if (fetchSource != null || sourceIncludes != null || sourceExcludes != null) {
            return new FetchSourceContext(fetchSource == null || fetchSource, sourceIncludes, sourceExcludes);
        }
        return null;
    }

    public static FetchSourceContext fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        switch (token) {
            case XContentParser.Token.VALUE_BOOLEAN -> {
                return new FetchSourceContext(parser.booleanValue());
            }
            case XContentParser.Token.VALUE_STRING -> {
                String[] includes = new String[] { parser.text() };
                return new FetchSourceContext(true, includes, null);
            }
            case XContentParser.Token.START_ARRAY -> {
                String[] includes = parseSourceFieldArray(parser, INCLUDES_FIELD, null).toArray(new String[0]);
                return new FetchSourceContext(true, includes, null);
            }
            case XContentParser.Token.START_OBJECT -> {
                return parseSourceObject(parser);
            }
            default -> {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Expected one of ["
                        + XContentParser.Token.VALUE_BOOLEAN
                        + ", "
                        + XContentParser.Token.VALUE_STRING
                        + ", "
                        + XContentParser.Token.START_ARRAY
                        + ", "
                        + XContentParser.Token.START_OBJECT
                        + "] but found ["
                        + token
                        + "]"
                );
            }
        }
        // MUST never reach here
    }

    public static FetchSourceContext parseSourceObject(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        Set<String> includes = Collections.emptySet();
        Set<String> excludes = Collections.emptySet();
        String currentFieldName = null;
        if (token != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected a " + XContentParser.Token.START_OBJECT + " but got a " + token + " in [" + parser.currentName() + "]."
            );
        }
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
                continue; // only field name is required in this iteration
            }
            if (currentFieldName == null) {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Expected a field name but got a " + token + " in [" + parser.currentName() + "]."
                );
            }
            // process field value
            switch (token) {
                case XContentParser.Token.START_ARRAY -> {
                    if (INCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        includes = parseSourceFieldArray(parser, INCLUDES_FIELD, excludes);
                    } else if (EXCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        excludes = parseSourceFieldArray(parser, EXCLUDES_FIELD, includes);
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "]."
                        );
                    }
                }
                case XContentParser.Token.VALUE_STRING -> {
                    if (INCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        String includeEntry = parser.text();
                        if (excludes.contains(includeEntry)) {
                            throw new ParsingException(parser.getTokenLocation(), AMBIGUOUS_FIELD_MESSAGE, includeEntry);
                        }
                        includes = Collections.singleton(includeEntry);
                    } else if (EXCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        String excludeEntry = parser.text();
                        if (includes.contains(excludeEntry)) {
                            throw new ParsingException(parser.getTokenLocation(), AMBIGUOUS_FIELD_MESSAGE, excludeEntry);
                        }
                        excludes = Collections.singleton(excludeEntry);
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "]."
                        );
                    }
                }
                default -> {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown key for a " + token + " in [" + currentFieldName + "].");
                }
            }
        }
        if (includes.isEmpty() && excludes.isEmpty()) {
            // no valid field names -> empty or unrecognized fields; not allowed
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected at least one of [" + INCLUDES_FIELD.getPreferredName() + "] or [" + EXCLUDES_FIELD.getPreferredName() + "]"
            );
        }
        return new FetchSourceContext(true, includes.toArray(new String[0]), excludes.toArray(new String[0]));
    }

    private static Set<String> parseSourceFieldArray(XContentParser parser, ParseField parseField, Set<String> opposite) throws IOException {
        Set<String> sourceArr = new LinkedHashSet<>(); // include or exclude lists, LinkedHashSet preserves the order of fields
        while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
            if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                String entry = parser.text();
                if (opposite != null && opposite.contains(entry)) {
                    throw new ParsingException(parser.getTokenLocation(), AMBIGUOUS_FIELD_MESSAGE, entry);
                }
                sourceArr.add(entry);
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Unknown key for a " + parser.currentToken() + " in [" + parser.currentName() + "]."
                );
            }
        }
        if (sourceArr.isEmpty()) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected at least one value for an array of [" + parseField.getPreferredName() + "]"
            );
        }
        return sourceArr;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (fetchSource) {
            builder.startObject();
            builder.array(INCLUDES_FIELD.getPreferredName(), includes);
            builder.array(EXCLUDES_FIELD.getPreferredName(), excludes);
            builder.endObject();
        } else {
            builder.value(false);
        }
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        FetchSourceContext that = (FetchSourceContext) o;

        if (fetchSource != that.fetchSource) return false;
        if (!Arrays.equals(excludes, that.excludes)) return false;
        if (!Arrays.equals(includes, that.includes)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (fetchSource ? 1 : 0);
        result = 31 * result + (includes != null ? Arrays.hashCode(includes) : 0);
        result = 31 * result + (excludes != null ? Arrays.hashCode(excludes) : 0);
        return result;
    }

    /**
     * Returns a filter function that expects the source map as an input and returns
     * the filtered map.
     */
    public Function<Map<String, ?>, Map<String, Object>> getFilter() {
        if (filter == null) {
            filter = XContentMapValues.filter(includes, excludes, true);
        }
        return filter;
    }
}
