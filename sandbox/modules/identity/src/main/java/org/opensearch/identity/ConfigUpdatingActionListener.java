/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;

import java.util.Objects;

public class ConfigUpdatingActionListener<Response> implements ActionListener<Response> {
    private final String[] cTypes;
    private final Client client;
    private final ActionListener<Response> delegate;

    public ConfigUpdatingActionListener(String[] cTypes, Client client, ActionListener<Response> delegate) {
        this.cTypes = Objects.requireNonNull(cTypes, "cTypes must not be null");
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    }

    @Override
    public void onResponse(Response response) {

        final ConfigUpdateRequest cur = new ConfigUpdateRequest(cTypes);

        client.execute(ConfigUpdateAction.INSTANCE, cur, new ActionListener<>() {
            @Override
            public void onResponse(final ConfigUpdateResponse ur) {
                if (ur.hasFailures()) {
                    delegate.onFailure(ur.failures().get(0));
                    return;
                }
                delegate.onResponse(response);
            }

            @Override
            public void onFailure(final Exception e) {
                delegate.onFailure(e);
            }
        });

    }

    @Override
    public void onFailure(Exception e) {
        delegate.onFailure(e);
    }

}
