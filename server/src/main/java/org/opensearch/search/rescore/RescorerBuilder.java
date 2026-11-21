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

package org.opensearch.search.rescore;

import org.opensearch.Version;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.NamedWriteable;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;

import java.io.IOException;
import java.util.Objects;

/**
 * The abstract base builder for instances of {@link RescorerBuilder}.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class RescorerBuilder<RB extends RescorerBuilder<RB>>
    implements
        NamedWriteable,
        ToXContentObject,
        Rewriteable<RescorerBuilder<RB>> {
    public static final int DEFAULT_WINDOW_SIZE = 10;

    protected Integer windowSize;

    protected RescoreReorderMode reorderMode;

    private static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    private static final ParseField REORDER_MODE_FIELD = new ParseField("reorder_mode");

    /**
     * Construct an empty RescoreBuilder.
     */
    public RescorerBuilder() {}

    /**
     * Read from a stream.
     */
    protected RescorerBuilder(StreamInput in) throws IOException {
        windowSize = in.readOptionalVInt();
        if (in.getVersion().onOrAfter(Version.V_3_8_0)) {
            reorderMode = in.readOptionalWriteable(RescoreReorderMode::readFromStream);
        }
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(this.windowSize);
        if (out.getVersion().onOrAfter(Version.V_3_8_0)) {
            out.writeOptionalWriteable(reorderMode);
        }
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @SuppressWarnings("unchecked")
    public RB windowSize(int windowSize) {
        this.windowSize = windowSize;
        return (RB) this;
    }

    public Integer windowSize() {
        return windowSize;
    }

    /**
     * Sets how hits are reordered after rescoring relative to hits beyond the
     * rescore window. Defaults to {@link RescoreReorderMode#LEGACY} when unset.
     */
    @SuppressWarnings("unchecked")
    public RB reorderMode(RescoreReorderMode reorderMode) {
        this.reorderMode = reorderMode;
        return (RB) this;
    }

    /**
     * How hits are reordered after rescoring, or {@code null} if not explicitly set.
     */
    public RescoreReorderMode reorderMode() {
        return reorderMode;
    }

    public static RescorerBuilder<?> parseFromXContent(XContentParser parser) throws IOException {
        String fieldName = null;
        RescorerBuilder<?> rescorer = null;
        Integer windowSize = null;
        RescoreReorderMode reorderMode = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (WINDOW_SIZE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    windowSize = parser.intValue();
                } else if (REORDER_MODE_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    reorderMode = RescoreReorderMode.fromString(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "rescore doesn't support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                rescorer = parser.namedObject(RescorerBuilder.class, fieldName, null);
            } else {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "] after [" + fieldName + "]");
            }
        }
        if (rescorer == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing rescore type");
        }
        if (windowSize != null) {
            rescorer.windowSize(windowSize.intValue());
        }
        if (reorderMode != null) {
            rescorer.reorderMode(reorderMode);
        }
        return rescorer;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (windowSize != null) {
            builder.field("window_size", windowSize);
        }
        if (reorderMode != null) {
            builder.field("reorder_mode", reorderMode.toString());
        }
        doXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    /**
     * Build the {@linkplain RescoreContext} that will be used to actually
     * execute the rescore against a particular shard.
     */
    public final RescoreContext buildContext(QueryShardContext context) throws IOException {
        int finalWindowSize = windowSize == null ? DEFAULT_WINDOW_SIZE : windowSize;
        RescoreContext rescoreContext = innerBuildContext(finalWindowSize, context);
        rescoreContext.setReorderMode(reorderMode == null ? RescoreReorderMode.LEGACY : reorderMode);
        return rescoreContext;
    }

    /**
     * Extensions override this to build the context that they need for rescoring.
     */
    protected abstract RescoreContext innerBuildContext(int windowSize, QueryShardContext context) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(windowSize, reorderMode);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RescorerBuilder<?> other = (RescorerBuilder<?>) obj;
        return Objects.equals(windowSize, other.windowSize) && Objects.equals(reorderMode, other.reorderMode);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this, true, true);
    }
}
