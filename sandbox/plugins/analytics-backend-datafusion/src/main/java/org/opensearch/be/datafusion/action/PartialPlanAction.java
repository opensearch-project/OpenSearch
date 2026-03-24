/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.be.datafusion.action;

import org.opensearch.action.ActionType;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Action type for partial plan execution on data nodes via stream transport.
 */
public class PartialPlanAction extends ActionType<PartialPlanAction.Response> {

    public static final String NAME = "indices:data/read/partial_plan";
    public static final PartialPlanAction INSTANCE = new PartialPlanAction();

    private PartialPlanAction() {
        super(NAME, Response::new);
    }

    /**
     * Empty response — actual data flows via native Arrow batches on stream transport.
     */
    public static class Response extends ActionResponse {
        public Response() {}

        public Response(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
