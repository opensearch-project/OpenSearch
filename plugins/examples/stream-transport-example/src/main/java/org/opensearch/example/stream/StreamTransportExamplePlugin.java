/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.example.stream;

import org.opensearch.action.ActionRequest;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.Plugin;

import java.util.List;

public class StreamTransportExamplePlugin extends Plugin implements ActionPlugin {

    public StreamTransportExamplePlugin() {}

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(
            new ActionHandler<>(StreamDataAction.INSTANCE, TransportStreamDataAction.class),
            new ActionHandler<>(NativeArrowStreamDataAction.INSTANCE, TransportNativeArrowStreamDataAction.class)
        );
    }
}
