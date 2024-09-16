/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionType;
import org.opensearch.action.ProtobufActionType;

/**
 * Transport action for executing a search
 *
 * @opensearch.internal
 */
public class ProtobufSearchAction extends ProtobufActionType<ProtobufSearchResponse> {

    public static final ProtobufSearchAction INSTANCE = new ProtobufSearchAction();
    public static final String NAME = "indices:data/read/search";

    private ProtobufSearchAction() {
        super(NAME, ProtobufSearchResponse::new);
    }

}
