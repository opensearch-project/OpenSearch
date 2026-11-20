/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.readonly;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexMetadata.APIBlock;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoveIndexBlockRequestTests extends OpenSearchTestCase {

    @BeforeClass
    public static void initializeIndexMetadataFirst() {
        // IndexMetadata and its nested APIBlock enum initialize each other and only work when IndexMetadata is initialized first, as it
        // always is on a running node. Do not let the first test that mentions APIBlock in this JVM decide.
        assertNotNull(IndexMetadata.INDEX_READ_ONLY_BLOCK);
    }

    /**
     * The blocks the add block API accepts, minus the one reserved for the disk watermark monitor and the one owned by the scale API.
     * Not a static field: {@link APIBlock} and IndexMetadata initialize each other, and must not be started from the enum.
     */
    private static List<APIBlock> removableBlocks() {
        return Arrays.asList(APIBlock.READ_ONLY, APIBlock.READ, APIBlock.WRITE, APIBlock.METADATA);
    }

    public void testSerialization() throws IOException {
        final APIBlock block = randomFrom(removableBlocks());
        final String[] indices = randomArray(1, 4, String[]::new, () -> randomAlphaOfLengthBetween(1, 10));
        final IndicesOptions indicesOptions = IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
        final RemoveIndexBlockRequest request = new RemoveIndexBlockRequest(block, indices).indicesOptions(indicesOptions);
        request.timeout(TimeValue.timeValueSeconds(randomIntBetween(1, 100)));
        request.clusterManagerNodeTimeout(TimeValue.timeValueSeconds(randomIntBetween(1, 100)));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final RemoveIndexBlockRequest deserialized = new RemoveIndexBlockRequest(in);
                assertThat(deserialized.getBlock(), equalTo(block));
                assertThat(deserialized.indices(), equalTo(indices));
                assertThat(deserialized.indicesOptions(), equalTo(indicesOptions));
                assertThat(deserialized.timeout(), equalTo(request.timeout()));
                assertThat(deserialized.clusterManagerNodeTimeout(), equalTo(request.clusterManagerNodeTimeout()));
            }
        }
    }

    public void testDefaultsMatchAddIndexBlockRequest() {
        final APIBlock block = randomFrom(removableBlocks());
        final RemoveIndexBlockRequest remove = new RemoveIndexBlockRequest(block, "test");
        final AddIndexBlockRequest add = new AddIndexBlockRequest(block, "test");
        assertThat(remove.indicesOptions(), equalTo(IndicesOptions.strictExpandOpen()));
        assertThat(remove.indicesOptions(), equalTo(add.indicesOptions()));
        assertThat(remove.timeout(), equalTo(add.timeout()));
        assertThat(remove.clusterManagerNodeTimeout(), equalTo(add.clusterManagerNodeTimeout()));
    }

    public void testIndicesReplaceable() {
        final RemoveIndexBlockRequest request = new RemoveIndexBlockRequest(APIBlock.WRITE, "a");
        assertThat(request.indices("b", "c"), equalTo(request));
        assertThat(request.indices(), arrayContaining("b", "c"));
    }

    public void testNullArgumentsAreRejected() {
        expectThrows(NullPointerException.class, () -> new RemoveIndexBlockRequest(null, "test"));
        expectThrows(NullPointerException.class, () -> new RemoveIndexBlockRequest(APIBlock.WRITE, (String[]) null));
        expectThrows(NullPointerException.class, () -> new RemoveIndexBlockRequest(APIBlock.WRITE, "test").indices((String[]) null));
    }

    public void testValidRequestsPassValidation() {
        for (APIBlock block : removableBlocks()) {
            assertNull(block.name(), new RemoveIndexBlockRequest(block, "test").validate());
        }
    }

    public void testValidationRequiresIndices() {
        final ActionRequestValidationException e = new RemoveIndexBlockRequest(APIBlock.WRITE).validate();
        assertNotNull(e);
        assertThat(e.validationErrors(), contains("index is missing"));
    }

    public void testValidationRejectsReadOnlyAllowDelete() {
        final ActionRequestValidationException e = new RemoveIndexBlockRequest(APIBlock.READ_ONLY_ALLOW_DELETE, "test").validate();
        assertNotNull(e);
        assertThat(e.validationErrors(), contains("read_only_allow_delete block is for internal use only"));
    }

    public void testValidationRejectsSearchOnlyAndPointsToScaleApi() {
        final ActionRequestValidationException e = new RemoveIndexBlockRequest(APIBlock.SEARCH_ONLY, "test").validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(1));
        assertThat(e.validationErrors().get(0), containsString("search_only"));
        assertThat(e.validationErrors().get(0), containsString("POST /{index}/_scale"));
    }

    public void testValidationReportsEveryProblem() {
        final ActionRequestValidationException e = new RemoveIndexBlockRequest(APIBlock.SEARCH_ONLY).validate();
        assertNotNull(e);
        assertThat(e.validationErrors().size(), equalTo(2));
    }
}
