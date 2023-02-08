/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.configuration;

import org.opensearch.action.ActionListener;
import org.opensearch.client.Client;

import java.util.Objects;

/**
 * Action listener for config update action
 * Delegates execution to another action listener
 * @param <Response> of {@link IdentityConfigUpdateResponse} type
 */
public class IdentityConfigUpdateActionListener<Response> implements ActionListener<Response> {
    private final String[] cTypes;
    private final Client client;
    private final ActionListener<Response> delegate;

    public IdentityConfigUpdateActionListener(String[] cTypes, Client client, ActionListener<Response> delegate) {
        this.cTypes = Objects.requireNonNull(cTypes, "cTypes must not be null");
        this.client = Objects.requireNonNull(client, "client must not be null");
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    }

    @Override
    public void onResponse(Response response) {

        final IdentityConfigUpdateRequest cur = new IdentityConfigUpdateRequest(cTypes);

        // execute update and delegate execution to other listeners
        client.execute(IdentityConfigUpdateAction.INSTANCE, cur, new ActionListener<>() {
            @Override
            public void onResponse(final IdentityConfigUpdateResponse ur) {
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
