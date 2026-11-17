/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

/** Tests for {@link MappedFieldType#isSearchableViaDocValues(IndexSettings)}. */
public class MappedFieldTypeFieldCapsTests extends OpenSearchTestCase {

    private static IndexSettings normalIndex() {
        return IndexSettingsModule.newIndexSettings("normal", Settings.EMPTY);
    }

    private static IndexSettings pluggableIndex() {
        return IndexSettingsModule.newIndexSettings(
            "pluggable",
            Settings.builder().put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true).build()
        );
    }

    private static NumberFieldMapper.NumberFieldType numberFieldType(boolean indexed, boolean hasDocValues) {
        return new NumberFieldMapper.NumberFieldType(
            "number",
            NumberFieldMapper.NumberType.LONG,
            indexed,
            false,
            hasDocValues,
            false,
            true,
            null,
            Collections.emptyMap()
        );
    }

    private static DateFieldMapper.DateFieldType dateFieldType(boolean indexed, boolean hasDocValues) {
        return new DateFieldMapper.DateFieldType(
            "date",
            indexed,
            false,
            hasDocValues,
            DateFieldMapper.getDefaultDateTimeFormatter(),
            DateFieldMapper.Resolution.MILLISECONDS,
            null,
            Collections.emptyMap()
        );
    }

    private static IpFieldMapper.IpFieldType ipFieldType(boolean indexed, boolean hasDocValues) {
        return new IpFieldMapper.IpFieldType("ip", indexed, false, hasDocValues, null, Collections.emptyMap());
    }

    /** Types that do not override the method are unaffected on any index. */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testBaseImplementationDelegatesToIsSearchable() {
        for (IndexSettings indexSettings : new IndexSettings[] { normalIndex(), pluggableIndex() }) {
            MappedFieldType indexedKeyword = new KeywordFieldMapper.KeywordFieldType("kw", true, true, Collections.emptyMap());
            assertTrue(indexedKeyword.isSearchableViaDocValues(indexSettings));

            MappedFieldType unindexedKeyword = new KeywordFieldMapper.KeywordFieldType("kw", false, true, Collections.emptyMap());
            assertFalse(unindexedKeyword.isSearchableViaDocValues(indexSettings));
        }
    }

    public void testOverriddenTypesUnchangedOnNormalIndex() {
        IndexSettings normal = normalIndex();

        assertTrue(numberFieldType(true, true).isSearchableViaDocValues(normal));
        assertFalse(numberFieldType(false, true).isSearchableViaDocValues(normal));

        assertTrue(dateFieldType(true, true).isSearchableViaDocValues(normal));
        assertFalse(dateFieldType(false, true).isSearchableViaDocValues(normal));

        assertTrue(ipFieldType(true, true).isSearchableViaDocValues(normal));
        assertFalse(ipFieldType(false, true).isSearchableViaDocValues(normal));

        assertTrue(new BooleanFieldMapper.BooleanFieldType("bool", true, true).isSearchableViaDocValues(normal));
        assertFalse(new BooleanFieldMapper.BooleanFieldType("bool", false, true).isSearchableViaDocValues(normal));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOverriddenTypesSearchableFromDocValuesOnPluggableIndex() {
        IndexSettings pluggable = pluggableIndex();
        assertTrue(pluggable.isPluggableDataFormatEnabled());

        assertTrue(numberFieldType(false, true).isSearchableViaDocValues(pluggable));
        assertTrue(dateFieldType(false, true).isSearchableViaDocValues(pluggable));
        assertTrue(ipFieldType(false, true).isSearchableViaDocValues(pluggable));
        assertTrue(new BooleanFieldMapper.BooleanFieldType("bool", false, true).isSearchableViaDocValues(pluggable));
    }

    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testOverriddenTypesNotSearchableWithoutDocValuesOnPluggableIndex() {
        IndexSettings pluggable = pluggableIndex();

        assertFalse(numberFieldType(false, false).isSearchableViaDocValues(pluggable));
        assertFalse(dateFieldType(false, false).isSearchableViaDocValues(pluggable));
        assertFalse(ipFieldType(false, false).isSearchableViaDocValues(pluggable));
        assertFalse(new BooleanFieldMapper.BooleanFieldType("bool", false, false).isSearchableViaDocValues(pluggable));
    }

    /** Query planning must keep seeing no search index. */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testIsSearchableItselfIsUnchanged() {
        IndexSettings pluggable = pluggableIndex();

        MappedFieldType number = numberFieldType(false, true);
        assertTrue(number.isSearchableViaDocValues(pluggable));
        assertFalse(number.isSearchable());

        MappedFieldType date = dateFieldType(false, true);
        assertTrue(date.isSearchableViaDocValues(pluggable));
        assertFalse(date.isSearchable());

        MappedFieldType ip = ipFieldType(false, true);
        assertTrue(ip.isSearchableViaDocValues(pluggable));
        assertFalse(ip.isSearchable());

        MappedFieldType bool = new BooleanFieldMapper.BooleanFieldType("bool", false, true);
        assertTrue(bool.isSearchableViaDocValues(pluggable));
        assertFalse(bool.isSearchable());
    }

    public void testNullIndexSettingsFallsBackToIsSearchable() {
        assertFalse(numberFieldType(false, true).isSearchableViaDocValues(null));
        assertTrue(numberFieldType(true, true).isSearchableViaDocValues(null));

        // Null index settings must fall back to isSearchable() for every overriding type, not just numbers.
        assertFalse(dateFieldType(false, true).isSearchableViaDocValues(null));
        assertFalse(ipFieldType(false, true).isSearchableViaDocValues(null));
        assertFalse(new BooleanFieldMapper.BooleanFieldType("bool", false, true).isSearchableViaDocValues(null));
    }

    /** FilterFieldType has no logic of its own here; it must return exactly what its delegate returns. */
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testFilterFieldTypeDelegatesIsSearchableViaDocValues() {
        MappedFieldType delegate = numberFieldType(false, true);
        FilterFieldType filtered = new FilterFieldType(delegate) {
            @Override
            public String typeName() {
                return delegate.typeName();
            }
        };
        IndexSettings pluggable = pluggableIndex();
        assertEquals(delegate.isSearchableViaDocValues(pluggable), filtered.isSearchableViaDocValues(pluggable));
    }
}
