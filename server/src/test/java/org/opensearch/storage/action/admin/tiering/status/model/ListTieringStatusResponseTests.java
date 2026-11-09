/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.action.admin.tiering.status.model;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.storage.action.tiering.status.model.ListTieringStatusResponse;
import org.opensearch.storage.action.tiering.status.model.TieringStatus;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListTieringStatusResponseTests extends OpenSearchTestCase {

    public void testConstructorWithList() {
        // Arrange
        List<TieringStatus> statusList = createDummyTieringStatusList();

        // Act
        ListTieringStatusResponse response = new ListTieringStatusResponse(statusList);

        // Assert
        assertNotNull(response.getTieringStatusList());
        assertEquals(2, response.getTieringStatusList().size());
        assertEquals("index1", response.getTieringStatusList().get(0).getIndexName());
        assertEquals("index2", response.getTieringStatusList().get(1).getIndexName());
    }

    public void testSerializationDeserialization() throws IOException {
        // Arrange
        List<TieringStatus> statusList = createDummyTieringStatusList();
        ListTieringStatusResponse originalResponse = new ListTieringStatusResponse(statusList);
        BytesStreamOutput output = new BytesStreamOutput();

        // Act
        originalResponse.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        ListTieringStatusResponse deserializedResponse = new ListTieringStatusResponse(input);

        // Assert
        assertEquals(originalResponse.getTieringStatusList().size(), deserializedResponse.getTieringStatusList().size());

        for (int i = 0; i < originalResponse.getTieringStatusList().size(); i++) {
            TieringStatus original = originalResponse.getTieringStatusList().get(i);
            TieringStatus deserialized = deserializedResponse.getTieringStatusList().get(i);

            assertEquals(original.getIndexName(), deserialized.getIndexName());
            assertEquals(original.getStatus(), deserialized.getStatus());
            assertEquals(original.getStartTime(), deserialized.getStartTime());
            assertEquals(original.getSource(), deserialized.getSource());
            assertEquals(original.getTarget(), deserialized.getTarget());
        }
    }

    public void testSerializationDeserializationEmptyList() throws IOException {
        // Arrange
        ListTieringStatusResponse originalResponse = new ListTieringStatusResponse(Collections.emptyList());
        BytesStreamOutput output = new BytesStreamOutput();

        // Act
        originalResponse.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        ListTieringStatusResponse deserializedResponse = new ListTieringStatusResponse(input);

        // Assert
        assertTrue(deserializedResponse.getTieringStatusList().isEmpty());
    }

    public void testGetTieringStatusList() {
        // Arrange
        List<TieringStatus> statusList = createDummyTieringStatusList();
        ListTieringStatusResponse response = new ListTieringStatusResponse(statusList);

        // Act
        List<TieringStatus> retrievedList = response.getTieringStatusList();

        // Assert
        assertNotNull(retrievedList);
        assertEquals(statusList.size(), retrievedList.size());
        assertEquals(statusList, retrievedList);
    }

    public void testSerializationWithLargeList() throws IOException {
        // Arrange
        List<TieringStatus> largeStatusList = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            TieringStatus status = new TieringStatus();
            status.setIndexName("index" + i);
            status.setStatus("RUNNING");
            status.setStartTime(System.currentTimeMillis());
            status.setSource("hot");
            status.setTarget("warm");
            largeStatusList.add(status);
        }

        ListTieringStatusResponse originalResponse = new ListTieringStatusResponse(largeStatusList);
        BytesStreamOutput output = new BytesStreamOutput();

        // Act
        originalResponse.writeTo(output);
        StreamInput input = output.bytes().streamInput();
        ListTieringStatusResponse deserializedResponse = new ListTieringStatusResponse(input);

        // Assert
        assertEquals(1000, deserializedResponse.getTieringStatusList().size());
        assertEquals("index0", deserializedResponse.getTieringStatusList().get(0).getIndexName());
        assertEquals("index999", deserializedResponse.getTieringStatusList().get(999).getIndexName());
    }

    private List<TieringStatus> createDummyTieringStatusList() {
        List<TieringStatus> statusList = new ArrayList<>();

        TieringStatus status1 = new TieringStatus();
        status1.setIndexName("index1");
        status1.setStatus("RUNNING");
        status1.setStartTime(System.currentTimeMillis());
        status1.setSource("hot");
        status1.setTarget("warm");

        TieringStatus status2 = new TieringStatus();
        status2.setIndexName("index2");
        status2.setStatus("COMPLETED");
        status2.setStartTime(System.currentTimeMillis());
        status2.setSource("warm");
        status2.setTarget("hot");

        statusList.add(status1);
        statusList.add(status2);

        return statusList;
    }
}
