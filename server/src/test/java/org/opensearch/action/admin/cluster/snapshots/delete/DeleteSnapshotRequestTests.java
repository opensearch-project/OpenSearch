/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.delete;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.test.AbstractWireSerializingTestCase;

public class DeleteSnapshotRequestTests extends AbstractWireSerializingTestCase<DeleteSnapshotRequest> {

    @Override
    protected DeleteSnapshotRequest createTestInstance() {
        DeleteSnapshotRequest request = new DeleteSnapshotRequest(
            randomAlphaOfLength(10),
            generateRandomStringArray(10, 10, false, false)
        );
        request.waitForCompletion(randomBoolean());
        if (randomBoolean()) {
            request.clusterManagerNodeTimeout(randomTimeValue());
        }
        return request;
    }

    @Override
    protected Writeable.Reader<DeleteSnapshotRequest> instanceReader() {
        return DeleteSnapshotRequest::new;
    }

    @Override
    protected DeleteSnapshotRequest mutateInstance(DeleteSnapshotRequest instance) {
        switch (randomInt(2)) {
            case 0:
                DeleteSnapshotRequest request = new DeleteSnapshotRequest(
                    randomAlphaOfLength(10),
                    instance.snapshots()
                );
                request.waitForCompletion(instance.waitForCompletion());
                request.clusterManagerNodeTimeout(instance.clusterManagerNodeTimeout());
                return request;
            case 1:
                request = new DeleteSnapshotRequest(
                    instance.repository(),
                    generateRandomStringArray(10, 10, false, false)
                );
                request.waitForCompletion(instance.waitForCompletion());
                request.clusterManagerNodeTimeout(instance.clusterManagerNodeTimeout());
                return request;
            case 2:
                request = new DeleteSnapshotRequest(
                    instance.repository(),
                    instance.snapshots()
                );
                request.waitForCompletion(!instance.waitForCompletion());
                request.clusterManagerNodeTimeout(instance.clusterManagerNodeTimeout());
                return request;
            default:
                throw new IllegalStateException("Unexpected value");
        }
    }

    public void testValidation() {
        // Test with missing repository
        DeleteSnapshotRequest request = new DeleteSnapshotRequest();
        request.snapshots("snapshot1");
        ActionRequestValidationException validationException = request.validate();
        assertNotNull(validationException);
        assertTrue(validationException.getMessage().contains("repository is missing"));

        // Test with missing snapshots
        request = new DeleteSnapshotRequest();
        request.repository("repo");
        validationException = request.validate();
        assertNotNull(validationException);
        assertTrue(validationException.getMessage().contains("snapshots are missing"));

        // Test with valid request
        request = new DeleteSnapshotRequest("repo", "snapshot1");
        validationException = request.validate();
        assertNull(validationException);
    }

    public void testWaitForCompletion() {
        DeleteSnapshotRequest request = new DeleteSnapshotRequest("repo", "snapshot1");
        
        // Default should be false
        assertFalse(request.waitForCompletion());
        
        // Test setting to true
        request.waitForCompletion(true);
        assertTrue(request.waitForCompletion());
        
        // Test setting to false
        request.waitForCompletion(false);
        assertFalse(request.waitForCompletion());
    }

    public void testFluentSetters() {
        DeleteSnapshotRequest request = new DeleteSnapshotRequest();
        
        // Test fluent API
        DeleteSnapshotRequest result = request.repository("test-repo")
            .snapshots("snap1", "snap2")
            .waitForCompletion(true);
        
        assertSame(request, result);
        assertEquals("test-repo", request.repository());
        assertArrayEquals(new String[]{"snap1", "snap2"}, request.snapshots());
        assertTrue(request.waitForCompletion());
    }
}
