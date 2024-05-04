/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ubi.ext;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchExtBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Subclass of {@link SearchExtBuilder} to access UBI parameters.
 */
public class UbiParametersExtBuilder extends SearchExtBuilder {

    /**
     * The name of the "ext" section containing UBI parameters.
     */
    public static final String UBI_PARAMETER_NAME = "ubi";

    private UbiParameters params;

    /**
     * Creates a new instance.
     */
    public UbiParametersExtBuilder() {}

    /**
     * Creates a new instance from a {@link StreamInput}.
     * @param input A {@link StreamInput} containing the parameters.
     * @throws IOException Thrown if the stream cannot be read.
     */
    public UbiParametersExtBuilder(StreamInput input) throws IOException {
        this.params = new UbiParameters(input);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getClass(), this.params);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof UbiParametersExtBuilder)) {
            return false;
        }

        return this.params.equals(((UbiParametersExtBuilder) obj).getParams());
    }

    @Override
    public String getWriteableName() {
        return UBI_PARAMETER_NAME;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        this.params.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(this.params);
    }

    /**
     * Parses the ubi section of the ext block.
     * @param parser A {@link XContentParser parser}.
     * @return The {@link UbiParameters paramers}.
     * @throws IOException Thrown if the UBI parameters cannot be read.
     */
    public static UbiParametersExtBuilder parse(XContentParser parser) throws IOException {
        final UbiParametersExtBuilder builder = new UbiParametersExtBuilder();
        builder.setParams(UbiParameters.parse(parser));
        return builder;
    }

    /**
     * Gets the {@link UbiParameters params}.
     * @return The {@link UbiParameters params}.
     */
    public UbiParameters getParams() {
        return params;
    }

    /**
     * Set the {@link UbiParameters params}.
     * @param params The {@link UbiParameters params}.
     */
    public void setParams(UbiParameters params) {
        this.params = params;
    }

}
