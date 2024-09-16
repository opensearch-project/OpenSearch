/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.main;

import org.opensearch.action.ProtobufActionType;

/**
 * The main OpenSearch Action
*
* @opensearch.internal
*/
public class ProtobufMainAction extends ProtobufActionType<ProtobufMainResponse> {

    public static final String NAME = "cluster:monitor/main";
    public static final ProtobufMainAction INSTANCE = new ProtobufMainAction();

    public ProtobufMainAction() {
        super(NAME, ProtobufMainResponse::new);
    }
}
