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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    private final boolean fetchSource;
    private final String[] includes;
    private final String[] excludes;
    private Function<Map<String, ?>, Map<String, Object>> filter;

    public FetchSourceContext(boolean fetchSource, String[] includes, String[] excludes) {
        this.fetchSource = fetchSource;
        this.includes = includes == null ? Strings.EMPTY_ARRAY : includes;
        this.excludes = excludes == null ? Strings.EMPTY_ARRAY : excludes;
    }

    public FetchSourceContext(boolean fetchSource) {
        this(fetchSource, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY);
    }

    public FetchSourceContext(StreamInput in) throws IOException {
        fetchSource = in.readBoolean();
        includes = in.readStringArray();
        excludes = in.readStringArray();
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
            return new FetchSourceContext(fetchSource == null ? true : fetchSource, sourceIncludes, sourceExcludes);
        }
        return null;
    }

    public static FetchSourceContext fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        boolean fetchSource = true;
        String[] includes = Strings.EMPTY_ARRAY;
        String[] excludes = Strings.EMPTY_ARRAY;
        if (token == XContentParser.Token.VALUE_BOOLEAN) {
            fetchSource = parser.booleanValue();
        } else if (token == XContentParser.Token.VALUE_STRING) {
            includes = new String[] { parser.text() };
        } else if (token == XContentParser.Token.START_ARRAY) {
            ArrayList<String> list = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                list.add(parser.text());
            }
            includes = list.toArray(new String[0]);
        } else if (token == XContentParser.Token.START_OBJECT) {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.START_ARRAY) {
                    if (INCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        List<String> includesList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                includesList.add(parser.text());
                            } else {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                    parser.getTokenLocation()
                                );
                            }
                        }
                        includes = includesList.toArray(new String[0]);
                    } else if (EXCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        List<String> excludesList = new ArrayList<>();
                        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                            if (token == XContentParser.Token.VALUE_STRING) {
                                excludesList.add(parser.text());
                            } else {
                                throw new ParsingException(
                                    parser.getTokenLocation(),
                                    "Unknown key for a " + token + " in [" + currentFieldName + "].",
                                    parser.getTokenLocation()
                                );
                            }
                        }
                        excludes = excludesList.toArray(new String[0]);
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation()
                        );
                    }
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if (INCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        includes = new String[] { parser.text() };
                    } else if (EXCLUDES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                        excludes = new String[] { parser.text() };
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + currentFieldName + "].",
                            parser.getTokenLocation()
                        );
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unknown key for a " + token + " in [" + currentFieldName + "].",
                        parser.getTokenLocation()
                    );
                }
            }
        } else {
            throw new ParsingException(
                parser.getTokenLocation(),
                "Expected one of ["
                    + XContentParser.Token.VALUE_BOOLEAN
                    + ", "
                    + XContentParser.Token.START_OBJECT
                    + "] but found ["
                    + token
                    + "]",
                parser.getTokenLocation()
            );
        }
        return new FetchSourceContext(fetchSource, includes, excludes);
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
            filter = XContentMapValues.filter(includes, excludes);
        }
        return filter;
    }
}
