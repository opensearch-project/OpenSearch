/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.transport;

import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.ratelimitting.admissioncontrol.AdmissionControlService;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportChannel;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class AdmissionControlTransportHandlerTests extends OpenSearchTestCase {
    AdmissionControlTransportHandler<TransportRequest> admissionControlTransportHandler;

    public void testHandlerInvoked() throws Exception {
        String action = "TEST";
        InterceptingRequestHandler<TransportRequest> handler = new InterceptingRequestHandler<>(action);
        admissionControlTransportHandler = new AdmissionControlTransportHandler<TransportRequest>(
            action,
            handler,
            mock(AdmissionControlService.class),
            false,
            null
        );
        admissionControlTransportHandler.messageReceived(mock(TransportRequest.class), mock(TransportChannel.class), mock(Task.class));
        assertEquals(1, handler.count);
    }

    public void testHandlerInvokedRejectedException() throws Exception {
        String action = "TEST";
        AdmissionControlService admissionControlService = mock(AdmissionControlService.class);
        doThrow(new OpenSearchRejectedExecutionException()).when(admissionControlService).applyTransportAdmissionControl(action, null);
        InterceptingRequestHandler<TransportRequest> handler = new InterceptingRequestHandler<>(action);
        admissionControlTransportHandler = new AdmissionControlTransportHandler<TransportRequest>(
            action,
            handler,
            admissionControlService,
            false,
            null
        );
        admissionControlTransportHandler.messageReceived(mock(TransportRequest.class), mock(TransportChannel.class), mock(Task.class));
        assertEquals(0, handler.count);
        handler.messageReceived(mock(TransportRequest.class), mock(TransportChannel.class), mock(Task.class));
        assertEquals(1, handler.count);
    }

    public void testHandlerInvokedRandomException() throws Exception {
        String action = "TEST";
        AdmissionControlService admissionControlService = mock(AdmissionControlService.class);
        doThrow(new NullPointerException()).when(admissionControlService).applyTransportAdmissionControl(action, null);
        InterceptingRequestHandler<TransportRequest> handler = new InterceptingRequestHandler<>(action);
        admissionControlTransportHandler = new AdmissionControlTransportHandler<TransportRequest>(
            action,
            handler,
            admissionControlService,
            false,
            null
        );
        try {
            admissionControlTransportHandler.messageReceived(mock(TransportRequest.class), mock(TransportChannel.class), mock(Task.class));
        } catch (Exception exception) {
            assertEquals(0, handler.count);
            handler.messageReceived(mock(TransportRequest.class), mock(TransportChannel.class), mock(Task.class));
        }
        assertEquals(1, handler.count);
    }

    private class InterceptingRequestHandler<T extends TransportRequest> implements TransportRequestHandler<T> {
        private final String action;
        public int count;

        public InterceptingRequestHandler(String action) {
            this.action = action;
            this.count = 0;
        }

        @Override
        public void messageReceived(T request, TransportChannel channel, Task task) throws Exception {
            this.count = this.count + 1;
        }
    }
}
