/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.hamcrest.MatcherAssert;

import static org.hamcrest.Matchers.nullValue;

public class ListViewNamesRequestTests extends AbstractWireSerializingTestCase<ListViewNamesAction.Request> {

    @Override
    protected Writeable.Reader<ListViewNamesAction.Request> instanceReader() {
        return ListViewNamesAction.Request::new;
    }

    @Override
    protected ListViewNamesAction.Request createTestInstance() {
        return new ListViewNamesAction.Request();
    }

    public void testValidateRequest() {
        final ListViewNamesAction.Request request = new ListViewNamesAction.Request();

        MatcherAssert.assertThat(request.validate(), nullValue());
    }

}
