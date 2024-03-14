/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.MatcherAssert;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;

public class DeleteViewRequestTests extends AbstractWireSerializingTestCase<DeleteViewAction.Request> {

    @Override
    protected Writeable.Reader<DeleteViewAction.Request> instanceReader() {
        return DeleteViewAction.Request::new;
    }

    @Override
    protected DeleteViewAction.Request createTestInstance() {
        return new DeleteViewAction.Request(randomAlphaOfLength(8));
    }

    public void testValidateRequest() {
        final DeleteViewAction.Request request = new DeleteViewAction.Request("my-view");

        MatcherAssert.assertThat(request.validate(), nullValue());
    }

    public void testValidateRequestWithoutName() {
        final DeleteViewAction.Request request = new DeleteViewAction.Request("");
        final ActionRequestValidationException e = request.validate();

        MatcherAssert.assertThat(e.validationErrors(), contains("name cannot be empty or null"));
    }

}
