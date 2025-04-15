/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import java.util.Map;

public class ExplanationPayload {
    public ExplanationPayload(Map<PayloadType, Object> explainPayload) {
        this.explainPayload = explainPayload;
    }

    public Map<PayloadType, Object> getExplainPayload() {
        return explainPayload;
    }

    private final Map<PayloadType, Object> explainPayload;

    public enum PayloadType {
        NORMALIZATION_PROCESSOR
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Map<PayloadType, Object> explainPayload;

        public Builder explainPayload(Map<PayloadType, Object> explainPayload) {
            this.explainPayload = explainPayload;
            return this;
        }

        public ExplanationPayload build() {
            return new ExplanationPayload(explainPayload);
        }
    }
}
