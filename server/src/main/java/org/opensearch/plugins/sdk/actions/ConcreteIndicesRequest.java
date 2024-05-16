/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins.sdk.actions;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * ConcreteIndicesRequest enables plugins to resolve index expressions to
 * a set of concrete indices.
 */
@ExperimentalApi
public class ConcreteIndicesRequest {

    private final String[] indexExpressions;
    private final IndicesOptions indicesOptions;

    private ConcreteIndicesRequest(Builder builder) {
        indexExpressions = Objects.requireNonNull(
            builder.indexExpressions,
            "indexExpressions is required"
        );

        indicesOptions = Objects.requireNonNull(
            builder.indicesOptions,
            "indicesOptions is required"
        );
    }

    public String[] indexExpressions() {
        return this.indexExpressions;
    }

    public IndicesOptions indicesOptions() {
        return this.indicesOptions;
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder of a ConcreteIndicesRequest.
     */
    @ExperimentalApi
    public static final class Builder {
        private String[] indexExpressions;
        private IndicesOptions indicesOptions;

        private Builder() {}

        public Builder indicesOptions(IndicesOptions indicesOptions) {
            this.indicesOptions = indicesOptions;
            return this;
        }

        public Builder indexExpressions(String... indexExpressions) {
            this.indexExpressions = indexExpressions;
            return this;
        }

        public ConcreteIndicesRequest build() {
            return new ConcreteIndicesRequest(this);
        }
    }
}
