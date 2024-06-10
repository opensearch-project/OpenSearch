/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote;

import org.opensearch.common.remote.RemoteWriteableEntity;

/**
 * Exception for Remote state transfer.
 */
public class RemoteStateTransferException extends RuntimeException {
    private RemoteWriteableEntity<?> entity;

    public RemoteStateTransferException(String errorDesc) {
        super(errorDesc);
    }

    public RemoteStateTransferException(String errorDesc, Throwable cause) {
        super(errorDesc, cause);
    }

    public RemoteStateTransferException(String errorDesc, RemoteWriteableEntity<?> entity) {
        super(errorDesc);
        this.entity = entity;
    }

    public RemoteStateTransferException(String errorDesc, RemoteWriteableEntity<?> entity, Throwable cause) {
        super(errorDesc, cause);
        this.entity = entity;
    }

    @Override
    public String toString() {
        String message = super.toString();
        if (entity != null) {
            message += ", failed entity:" + entity;
        }
        return message;
    }
}
