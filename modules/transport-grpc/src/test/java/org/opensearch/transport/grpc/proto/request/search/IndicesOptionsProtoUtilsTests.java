/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.protobufs.ExpandWildcard;
import org.opensearch.protobufs.SearchRequest;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import static org.opensearch.action.support.IndicesOptions.WildcardStates;

public class IndicesOptionsProtoUtilsTests extends OpenSearchTestCase {

    public void testFromRequestWithDefaultSettings() {
        // Create a SearchRequest with no indices options
        SearchRequest searchRequest = SearchRequest.newBuilder().build();

        // Create default settings
        IndicesOptions defaultSettings = IndicesOptions.strictExpandOpenAndForbidClosed();

        // Call the method under test
        IndicesOptions indicesOptions = IndicesOptionsProtoUtils.fromRequest(searchRequest, defaultSettings);

        // Verify the result
        assertNotNull("IndicesOptions should not be null", indicesOptions);
        assertEquals("Should return default settings", defaultSettings, indicesOptions);
    }

    public void testFromRequestWithCustomSettings() {
        // Create a SearchRequest with custom indices options
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .setIgnoreUnavailable(true)
            .setAllowNoIndices(false)
            .setIgnoreThrottled(true)
            .addExpandWildcards(ExpandWildcard.EXPAND_WILDCARD_OPEN)
            .addExpandWildcards(ExpandWildcard.EXPAND_WILDCARD_CLOSED)
            .build();

        // Create default settings
        IndicesOptions defaultSettings = IndicesOptions.strictExpandOpenAndForbidClosed();

        // Call the method under test
        IndicesOptions indicesOptions = IndicesOptionsProtoUtils.fromRequest(searchRequest, defaultSettings);

        // Verify the result
        assertNotNull("IndicesOptions should not be null", indicesOptions);
        assertTrue("Should ignore unavailable", indicesOptions.ignoreUnavailable());
        assertFalse("Should not allow no indices", indicesOptions.allowNoIndices());
        assertTrue("Should ignore throttled", indicesOptions.ignoreThrottled());
        assertTrue("Should expand open", indicesOptions.expandWildcardsOpen());
        assertTrue("Should expand closed", indicesOptions.expandWildcardsClosed());
        assertFalse("Should not expand hidden", indicesOptions.expandWildcardsHidden());
    }

    public void testFromProtoParametersWithPartialSettings() {
        // Create a SearchRequest with partial indices options
        SearchRequest searchRequest = SearchRequest.newBuilder()
            .setIgnoreUnavailable(true)
            // allowNoIndices not set
            .setIgnoreThrottled(true)
            .addExpandWildcards(ExpandWildcard.EXPAND_WILDCARD_OPEN)
            .build();

        // Create default settings
        IndicesOptions defaultSettings = IndicesOptions.strictExpandOpenAndForbidClosed();

        // Call the method under test
        IndicesOptions indicesOptions = IndicesOptionsProtoUtils.fromProtoParameters(searchRequest, defaultSettings);

        // Verify the result
        assertNotNull("IndicesOptions should not be null", indicesOptions);
        assertTrue("Should ignore unavailable", indicesOptions.ignoreUnavailable());
        assertEquals("Should use default for allowNoIndices", defaultSettings.allowNoIndices(), indicesOptions.allowNoIndices());
        assertTrue("Should ignore throttled", indicesOptions.ignoreThrottled());
        assertTrue("Should expand open", indicesOptions.expandWildcardsOpen());
        assertFalse("Should not expand closed", indicesOptions.expandWildcardsClosed());
        assertFalse("Should not expand hidden", indicesOptions.expandWildcardsHidden());
    }

    public void testParseProtoParameterWithEmptyList() {
        // Create an empty list of ExpandWildcard
        List<ExpandWildcard> wildcardList = Collections.emptyList();

        // Create default states
        EnumSet<WildcardStates> defaultStates = EnumSet.of(WildcardStates.OPEN);

        // Call the method under test
        EnumSet<WildcardStates> states = IndicesOptionsProtoUtils.parseProtoParameter(wildcardList, defaultStates);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertEquals("Should return default states", defaultStates, states);
    }

    public void testParseProtoParameterWithSingleValue() {
        // Create a list with a single ExpandWildcard
        List<ExpandWildcard> wildcardList = Collections.singletonList(ExpandWildcard.EXPAND_WILDCARD_CLOSED);

        // Create default states
        EnumSet<WildcardStates> defaultStates = EnumSet.of(WildcardStates.OPEN);

        // Call the method under test
        EnumSet<WildcardStates> states = IndicesOptionsProtoUtils.parseProtoParameter(wildcardList, defaultStates);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertEquals("Should have 1 state", 1, states.size());
        assertTrue("Should contain CLOSED", states.contains(WildcardStates.CLOSED));
        assertFalse("Should not contain OPEN", states.contains(WildcardStates.OPEN));
    }

    public void testParseProtoParameterWithMultipleValues() {
        // Create a list with multiple ExpandWildcard values
        List<ExpandWildcard> wildcardList = Arrays.asList(ExpandWildcard.EXPAND_WILDCARD_OPEN, ExpandWildcard.EXPAND_WILDCARD_HIDDEN);

        // Create default states
        EnumSet<WildcardStates> defaultStates = EnumSet.of(WildcardStates.CLOSED);

        // Call the method under test
        EnumSet<WildcardStates> states = IndicesOptionsProtoUtils.parseProtoParameter(wildcardList, defaultStates);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertEquals("Should have 2 states", 2, states.size());
        assertTrue("Should contain OPEN", states.contains(WildcardStates.OPEN));
        assertTrue("Should contain HIDDEN", states.contains(WildcardStates.HIDDEN));
        assertFalse("Should not contain CLOSED", states.contains(WildcardStates.CLOSED));
    }

    public void testParseProtoParameterWithNoneValue() {
        // Create a list with NONE value
        List<ExpandWildcard> wildcardList = Collections.singletonList(ExpandWildcard.EXPAND_WILDCARD_NONE);

        // Create default states with all values
        EnumSet<WildcardStates> defaultStates = EnumSet.allOf(WildcardStates.class);

        // Call the method under test
        EnumSet<WildcardStates> states = IndicesOptionsProtoUtils.parseProtoParameter(wildcardList, defaultStates);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertTrue("Should be empty", states.isEmpty());
    }

    public void testParseProtoParameterWithAllValue() {
        // Create a list with ALL value
        List<ExpandWildcard> wildcardList = Collections.singletonList(ExpandWildcard.EXPAND_WILDCARD_ALL);

        // Create default states with no values
        EnumSet<WildcardStates> defaultStates = EnumSet.noneOf(WildcardStates.class);

        // Call the method under test
        EnumSet<WildcardStates> states = IndicesOptionsProtoUtils.parseProtoParameter(wildcardList, defaultStates);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertEquals("Should have all states", EnumSet.allOf(WildcardStates.class), states);
    }

    public void testParseProtoParameterWithNoneFollowedByValues() {
        // Create a list with NONE followed by other values
        List<ExpandWildcard> wildcardList = Arrays.asList(
            ExpandWildcard.EXPAND_WILDCARD_NONE,
            ExpandWildcard.EXPAND_WILDCARD_OPEN,
            ExpandWildcard.EXPAND_WILDCARD_CLOSED
        );

        // Create default states
        EnumSet<WildcardStates> defaultStates = EnumSet.of(WildcardStates.HIDDEN);

        // Call the method under test
        EnumSet<WildcardStates> states = IndicesOptionsProtoUtils.parseProtoParameter(wildcardList, defaultStates);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertEquals("Should have 2 states", 2, states.size());
        assertTrue("Should contain OPEN", states.contains(WildcardStates.OPEN));
        assertTrue("Should contain CLOSED", states.contains(WildcardStates.CLOSED));
        assertFalse("Should not contain HIDDEN", states.contains(WildcardStates.HIDDEN));
    }

    public void testParseProtoParameterWithValuesFollowedByNone() {
        // Create a list with values followed by NONE
        List<ExpandWildcard> wildcardList = Arrays.asList(
            ExpandWildcard.EXPAND_WILDCARD_OPEN,
            ExpandWildcard.EXPAND_WILDCARD_CLOSED,
            ExpandWildcard.EXPAND_WILDCARD_NONE
        );

        // Create default states
        EnumSet<WildcardStates> defaultStates = EnumSet.of(WildcardStates.HIDDEN);

        // Call the method under test
        EnumSet<WildcardStates> states = IndicesOptionsProtoUtils.parseProtoParameter(wildcardList, defaultStates);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertTrue("Should be empty", states.isEmpty());
    }

    public void testUpdateSetForValueWithOpen() {
        // Create an empty EnumSet
        EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);

        // Call the method under test
        IndicesOptionsProtoUtils.updateSetForValue(states, ExpandWildcard.EXPAND_WILDCARD_OPEN);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertEquals("Should have 1 state", 1, states.size());
        assertTrue("Should contain OPEN", states.contains(WildcardStates.OPEN));
    }

    public void testUpdateSetForValueWithClosed() {
        // Create an empty EnumSet
        EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);

        // Call the method under test
        IndicesOptionsProtoUtils.updateSetForValue(states, ExpandWildcard.EXPAND_WILDCARD_CLOSED);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertEquals("Should have 1 state", 1, states.size());
        assertTrue("Should contain CLOSED", states.contains(WildcardStates.CLOSED));
    }

    public void testUpdateSetForValueWithHidden() {
        // Create an empty EnumSet
        EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);

        // Call the method under test
        IndicesOptionsProtoUtils.updateSetForValue(states, ExpandWildcard.EXPAND_WILDCARD_HIDDEN);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertEquals("Should have 1 state", 1, states.size());
        assertTrue("Should contain HIDDEN", states.contains(WildcardStates.HIDDEN));
    }

    public void testUpdateSetForValueWithNone() {
        // Create an EnumSet with all values
        EnumSet<WildcardStates> states = EnumSet.allOf(WildcardStates.class);

        // Call the method under test
        IndicesOptionsProtoUtils.updateSetForValue(states, ExpandWildcard.EXPAND_WILDCARD_NONE);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertTrue("Should be empty", states.isEmpty());
    }

    public void testUpdateSetForValueWithAll() {
        // Create an empty EnumSet
        EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);

        // Call the method under test
        IndicesOptionsProtoUtils.updateSetForValue(states, ExpandWildcard.EXPAND_WILDCARD_ALL);

        // Verify the result
        assertNotNull("States should not be null", states);
        assertEquals("Should have all states", EnumSet.allOf(WildcardStates.class), states);
    }

    public void testUpdateSetForValueWithInvalidValue() {
        // Create an empty EnumSet
        EnumSet<WildcardStates> states = EnumSet.noneOf(WildcardStates.class);

        // Call the method under test with UNRECOGNIZED value, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> IndicesOptionsProtoUtils.updateSetForValue(states, ExpandWildcard.UNRECOGNIZED)
        );

        assertTrue(
            "Exception message should mention no valid expand wildcard value",
            exception.getMessage().contains("No valid expand wildcard value")
        );
    }
}
