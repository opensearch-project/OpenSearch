/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.termsenum;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchModule;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.tasks.Task;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.List;
import java.util.Map;

public class TermsEnumRequestTests extends AbstractWireSerializingTestCase<TermsEnumRequest> {

    @Override
    protected Writeable.Reader<TermsEnumRequest> instanceReader() {
        return TermsEnumRequest::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, List.of()).getNamedWriteables());
    }

    @Override
    protected TermsEnumRequest createTestInstance() {
        String[] indices = new String[randomIntBetween(1, 3)];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = randomAlphaOfLengthBetween(1, 8);
        }
        TermsEnumRequest request = new TermsEnumRequest(indices).field(randomAlphaOfLengthBetween(1, 10))
            .string(randomAlphaOfLengthBetween(0, 20))
            .size(randomIntBetween(1, 1000))
            .caseInsensitive(randomBoolean())
            .timeout(TimeValue.timeValueMillis(randomIntBetween(1, 5000)))
            .indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        if (randomBoolean()) {
            request.indexFilter(QueryBuilders.termQuery("f", "v"));
        }
        if (randomBoolean()) {
            request.searchAfter(randomAlphaOfLengthBetween(1, 10));
        }
        return request;
    }

    @Override
    protected TermsEnumRequest mutateInstance(TermsEnumRequest in) {
        TermsEnumRequest copy = new TermsEnumRequest(in.indices()).field(in.field())
            .string(in.string())
            .size(in.size())
            .caseInsensitive(in.caseInsensitive())
            .timeout(in.timeout())
            .indexFilter(in.indexFilter())
            .searchAfter(in.searchAfter())
            .indicesOptions(in.indicesOptions());
        switch (randomIntBetween(0, 8)) {
            case 0:
                copy.indices(in.indices().length == 1 ? new String[] { in.indices()[0], "extra" } : new String[] { in.indices()[0] });
                break;
            case 1:
                copy.field(in.field() + "x");
                break;
            case 2:
                copy.string(in.string() + "x");
                break;
            case 3:
                copy.size(in.size() + 1);
                break;
            case 4:
                copy.caseInsensitive(in.caseInsensitive() == false);
                break;
            case 5:
                copy.timeout(TimeValue.timeValueMillis(in.timeout().millis() + 1));
                break;
            case 6:
                copy.indexFilter(in.indexFilter() == null ? QueryBuilders.termQuery("f", "v") : null);
                break;
            case 7:
                copy.indicesOptions(
                    in.indicesOptions().equals(IndicesOptions.strictExpandOpen())
                        ? IndicesOptions.lenientExpandOpen()
                        : IndicesOptions.strictExpandOpen()
                );
                break;
            default:
                copy.searchAfter(in.searchAfter() == null ? "a" : in.searchAfter() + "x");
                break;
        }
        return copy;
    }

    public void testDefaults() {
        TermsEnumRequest request = new TermsEnumRequest("idx");
        assertEquals(10, request.size());
        assertEquals("", request.string());
        assertFalse(request.caseInsensitive());
        assertEquals(TimeValue.timeValueSeconds(1), request.timeout());
        assertNull(request.searchAfter());
        assertNull(request.indexFilter());
        assertEquals(IndicesOptions.strictExpandOpen(), request.indicesOptions());
        assertTrue(request.includeDataStreams());
        assertNull(request.string(null).field());
        assertEquals("", request.string());
    }

    public void testValidateRequiresField() {
        ActionRequestValidationException e = new TermsEnumRequest("idx").validate();
        assertNotNull(e);
        assertEquals(List.of("[field] is required"), e.validationErrors());
    }

    public void testValidateSize() {
        ActionRequestValidationException e = new TermsEnumRequest("idx").field("f").size(0).validate();
        assertNotNull(e);
        assertEquals(List.of("[size] must be at least 1 but was [0]"), e.validationErrors());
        assertNull(new TermsEnumRequest("idx").field("f").size(1).validate());
    }

    public void testValidateStringLength() {
        assertNull(new TermsEnumRequest("idx").field("f").string("a".repeat(1000)).validate());
        ActionRequestValidationException e = new TermsEnumRequest("idx").field("f").string("a".repeat(1001)).validate();
        assertNotNull(e);
        assertEquals(List.of("[string] must be at most [1000] characters but was [1001]"), e.validationErrors());
    }

    public void testValidateTimeout() {
        ActionRequestValidationException zero = new TermsEnumRequest("idx").field("f").timeout(TimeValue.ZERO).validate();
        assertNotNull(zero);
        assertEquals(List.of("[timeout] must be at least 1ms but was [0s]"), zero.validationErrors());
        ActionRequestValidationException negative = new TermsEnumRequest("idx").field("f").timeout(TimeValue.MINUS_ONE).validate();
        assertNotNull(negative);
        assertEquals(1, negative.validationErrors().size());
        assertTrue(negative.validationErrors().get(0).startsWith("[timeout] must be at least 1ms but was ["));
        assertNull(new TermsEnumRequest("idx").field("f").timeout(TimeValue.timeValueMillis(1)).validate());
    }

    public void testValidateRejectsSubMillisecondTimeout() {
        // the coordinator counts whole milliseconds, so a positive timeout below 1ms cannot be honored
        ActionRequestValidationException e = new TermsEnumRequest("idx").field("f").timeout(TimeValue.timeValueNanos(500_000)).validate();
        assertNotNull(e);
        assertEquals(List.of("[timeout] must be at least 1ms but was [500micros]"), e.validationErrors());
        e = new TermsEnumRequest("idx").field("f").timeout(TimeValue.timeValueNanos(999_999)).validate();
        assertNotNull(e);
        assertEquals(1, e.validationErrors().size());
        assertTrue(e.validationErrors().get(0).startsWith("[timeout] must be at least 1ms but was ["));
    }

    public void testCreateTaskCancelsChildren() {
        Task task = new TermsEnumRequest("idx").field("f")
            .createTask(1L, "transport", "indices:data/read/terms_enum", TaskId.EMPTY_TASK_ID, Map.of());
        assertTrue(task instanceof CancellableTask);
        assertTrue(((CancellableTask) task).shouldCancelChildrenOnCancellation());
    }

    public void testDescription() {
        assertEquals("terms_enum indices[a,b], field[f]", new TermsEnumRequest("a", "b").field("f").getDescription());
    }
}
