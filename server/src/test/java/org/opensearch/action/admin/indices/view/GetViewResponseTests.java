/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.view;

import org.opensearch.cluster.metadata.View;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.TreeSet;

public class GetViewResponseTests extends AbstractWireSerializingTestCase<GetViewAction.Response> {

    @Override
    protected Writeable.Reader<GetViewAction.Response> instanceReader() {
        return GetViewAction.Response::new;
    }

    @Override
    protected GetViewAction.Response createTestInstance() {
        return new GetViewAction.Response(
            new View(
                randomAlphaOfLength(8),
                randomAlphaOfLength(8),
                randomLong(),
                randomLong(),
                new TreeSet<>(randomList(5, () -> new View.Target(randomAlphaOfLength(8))))
            )
        );
    }
}
