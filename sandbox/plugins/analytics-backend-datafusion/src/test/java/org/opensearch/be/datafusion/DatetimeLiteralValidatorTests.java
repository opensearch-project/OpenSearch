/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.test.OpenSearchTestCase;

public class DatetimeLiteralValidatorTests extends OpenSearchTestCase {

    public void testValidDatesAccepted() {
        DatetimeLiteralValidator.validate("2024-01-15", DatetimeLiteralValidator.Kind.DATE);
        DatetimeLiteralValidator.validate("1984-04-12", DatetimeLiteralValidator.Kind.DATE);
    }

    public void testInvalidDateRejectedWithDateHint() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DatetimeLiteralValidator.validate("2025-13-02", DatetimeLiteralValidator.Kind.DATE)
        );
        assertEquals("date:2025-13-02 in unsupported format, please use 'yyyy-MM-dd'", e.getMessage());
    }

    public void testTimeStringRejectedAsDate() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DatetimeLiteralValidator.validate("09:07:42", DatetimeLiteralValidator.Kind.DATE)
        );
        assertEquals("date:09:07:42 in unsupported format, please use 'yyyy-MM-dd'", e.getMessage());
    }

    public void testValidTimesAccepted() {
        DatetimeLiteralValidator.validate("09:07:42", DatetimeLiteralValidator.Kind.TIME);
        DatetimeLiteralValidator.validate("09:07:42.12345", DatetimeLiteralValidator.Kind.TIME);
        DatetimeLiteralValidator.validate("23:59:59", DatetimeLiteralValidator.Kind.TIME);
    }

    public void testInvalidTimeRejectedWithTimeHint() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DatetimeLiteralValidator.validate("16:00:61", DatetimeLiteralValidator.Kind.TIME)
        );
        assertEquals("time:16:00:61 in unsupported format, please use 'HH:mm:ss[.SSSSSSSSS]'", e.getMessage());
    }

    public void testDateStringRejectedAsTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DatetimeLiteralValidator.validate("1984-04-12", DatetimeLiteralValidator.Kind.TIME)
        );
        assertEquals("time:1984-04-12 in unsupported format, please use 'HH:mm:ss[.SSSSSSSSS]'", e.getMessage());
    }

    public void testTimestampAcceptsBareDate() {
        DatetimeLiteralValidator.validate("2024-01-15", DatetimeLiteralValidator.Kind.TIMESTAMP);
    }

    public void testTimestampRejectsBareTime() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DatetimeLiteralValidator.validate("09:07:42", DatetimeLiteralValidator.Kind.TIMESTAMP)
        );
        assertEquals("timestamp:09:07:42 in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'", e.getMessage());
    }

    public void testTimestampAcceptsSpaceSeparator() {
        DatetimeLiteralValidator.validate("2024-01-15 09:07:42", DatetimeLiteralValidator.Kind.TIMESTAMP);
    }

    public void testInvalidTimestampRejectedWithTimestampHint() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> DatetimeLiteralValidator.validate("2025-13-02 garbage", DatetimeLiteralValidator.Kind.TIMESTAMP)
        );
        assertEquals("timestamp:2025-13-02 garbage in unsupported format, please use 'yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]'", e.getMessage());
    }
}
