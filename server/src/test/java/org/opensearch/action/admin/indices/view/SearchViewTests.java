/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SearchViewTests extends AbstractWireSerializingTestCase<SearchViewAction.Request> {

    @Override
    protected Writeable.Reader<SearchViewAction.Request> instanceReader() {
        return SearchViewAction.Request::new;
    }

    @Override
    protected SearchViewAction.Request createTestInstance() {
        try {
            return SearchViewAction.createRequestWith(randomAlphaOfLength(8), new SearchRequest());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testValidateRequest() throws IOException {
        final SearchViewAction.Request request = SearchViewAction.createRequestWith("my-view", new SearchRequest());
        assertNull(request.validate());
    }

    public void testValidateRequestWithoutName() {
        final SearchViewAction.Request request = new SearchViewAction.Request((String) null);
        ActionRequestValidationException e = request.validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("View is required"));
    }

}
