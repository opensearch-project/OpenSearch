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
import org.hamcrest.MatcherAssert;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class SearchViewRequestTests extends AbstractWireSerializingTestCase<SearchViewAction.Request> {

    @Override
    protected Writeable.Reader<SearchViewAction.Request> instanceReader() {
        return SearchViewAction.Request::new;
    }

    @Override
    protected SearchViewAction.Request createTestInstance() {
        try {
            return new SearchViewAction.Request(randomAlphaOfLength(8), new SearchRequest());
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void testValidateRequest() throws IOException {
        final SearchViewAction.Request request = new SearchViewAction.Request("my-view", new SearchRequest());
        MatcherAssert.assertThat(request.validate(), nullValue());
    }

    public void testValidateRequestWithoutName() {
        final SearchViewAction.Request request = new SearchViewAction.Request((String) null, new SearchRequest());
        final ActionRequestValidationException e = request.validate();

        MatcherAssert.assertThat(e.validationErrors().size(), equalTo(1));
        MatcherAssert.assertThat(e.validationErrors().get(0), containsString("View is required"));
    }

}
