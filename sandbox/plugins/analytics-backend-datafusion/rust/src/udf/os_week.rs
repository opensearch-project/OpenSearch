/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! `os_week(date [, mode])` — MySQL `WEEK()` semantics (modes 0..7), used by PPL's
//! `WEEK` / `WEEK_OF_YEAR`. DataFusion's `date_part('week', ts)` is ISO-only and disagrees
//! with MySQL's default mode 0 (Sunday-first).

use std::any::Any;
use std::sync::Arc;

use chrono::{Datelike, NaiveDate, Weekday};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Builder};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, Result};
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};

use super::udf_identity;

pub fn register_all(ctx: &SessionContext) {
    ctx.register_udf(ScalarUDF::from(OsWeekUdf::new()));
}

#[derive(Debug)]
pub struct OsWeekUdf {
    signature: Signature,
}

impl OsWeekUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(OsWeekUdf, "os_week");

impl ScalarUDFImpl for OsWeekUdf {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "os_week"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return plan_err!("os_week expects 1 or 2 arguments, got {}", arg_types.len());
        }
        Ok(DataType::Int32)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return plan_err!("os_week expects 1 or 2 arguments, got {}", arg_types.len());
        }
        let mut out = Vec::with_capacity(arg_types.len());
        match &arg_types[0] {
            DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View => out.push(DataType::Date32),
            other => return plan_err!("os_week: arg 0 expected date/timestamp/string, got {other:?}"),
        }
        if arg_types.len() == 2 {
            match &arg_types[1] {
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => out.push(DataType::Int32),
                other => return plan_err!("os_week: arg 1 expected integer mode, got {other:?}"),
            }
        }
        Ok(out)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let n = args.number_rows;
        let date_arg = args.args[0].clone().into_array(n)?;
        let mode_arg = if args.args.len() == 2 {
            Some(args.args[1].clone().into_array(n)?)
        } else {
            None
        };
        let dates = date_arg
            .as_any()
            .downcast_ref::<datafusion::arrow::array::Date32Array>()
            .ok_or_else(|| datafusion::error::DataFusionError::Internal(
                "os_week: arg 0 not coerced to Date32".to_string(),
            ))?;
        let mut builder = Int32Builder::with_capacity(n);
        for i in 0..n {
            if dates.is_null(i) {
                builder.append_null();
                continue;
            }
            let mode = match &mode_arg {
                Some(arr) if arr.is_null(i) => 0,
                Some(arr) => arr.as_primitive::<datafusion::arrow::datatypes::Int32Type>().value(i),
                None => 0,
            };
            let days = dates.value(i);
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719_163)
                .ok_or_else(|| datafusion::error::DataFusionError::Execution(
                    format!("os_week: invalid Date32 day count {days}"),
                ))?;
            builder.append_value(os_week_number(date, mode) as i32);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// MySQL `WEEK()` modes 0..7; see
/// https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_week.
/// Modes 1/3/4/6 use the ≥4-days-in-year rule; the rest use simple counting.
fn os_week_number(date: NaiveDate, mode: i32) -> u32 {
    let mode = mode.rem_euclid(8) as u32;
    let (start, four_day_rule, range_starts_at_one) = match mode {
        0 => (Weekday::Sun, false, false),
        1 => (Weekday::Mon, true, false),
        2 => (Weekday::Sun, false, true),
        3 => (Weekday::Mon, true, true),
        4 => (Weekday::Sun, true, false),
        5 => (Weekday::Mon, false, false),
        6 => (Weekday::Sun, true, true),
        7 => (Weekday::Mon, false, true),
        _ => unreachable!(),
    };
    if four_day_rule {
        week_number_iso_fold(date, start)
    } else if range_starts_at_one {
        // Range 1-53: week 0 (dates before the first start-day of the year)
        // rolls into the prior year's last week.
        let wn = week_number_simple(date, start);
        if wn == 0 {
            let last_of_prior = NaiveDate::from_ymd_opt(date.year() - 1, 12, 31).unwrap();
            week_number_simple(last_of_prior, start)
        } else {
            wn
        }
    } else {
        week_number_simple(date, start)
    }
}

/// Modes 0/1: simple count. Week 1 starts at the first `start`-day of the year;
/// dates before that are week 0.
fn week_number_simple(date: NaiveDate, start: Weekday) -> u32 {
    let jan1 = NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap();
    let offset = days_until_start(jan1.weekday(), start);
    let first_doy = 1 + offset;
    let doy = date.ordinal();
    if doy < first_doy {
        0
    } else {
        (doy - first_doy) / 7 + 1
    }
}

/// Modes 2/3: week 1 must contain at least 4 days of the new year; otherwise
/// the early days roll into the prior year's last week (52 or 53).
fn week_number_iso_fold(date: NaiveDate, start: Weekday) -> u32 {
    let year = date.year();
    let week1_start = first_anchored_week_start(year, start);
    if date < week1_start {
        // Roll into prior year's last week.
        let prev_week1 = first_anchored_week_start(year - 1, start);
        let days = (date - prev_week1).num_days();
        (days / 7) as u32 + 1
    } else {
        let next_week1 = first_anchored_week_start(year + 1, start);
        if date >= next_week1 {
            1
        } else {
            let days = (date - week1_start).num_days();
            (days / 7) as u32 + 1
        }
    }
}

/// First day of the year's week 1 under the "≥4 days of new year" anchor —
/// MySQL modes 2/3 (and ISO 8601, with start=Mon).
fn first_anchored_week_start(year: i32, start: Weekday) -> NaiveDate {
    let jan1 = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let offset = days_until_start(jan1.weekday(), start);
    if offset <= 3 {
        // Jan 1's week-start lands on Jan {1 - offset}; that week has ≥4 days of new year.
        jan1 - chrono::Duration::days(offset as i64)
    } else {
        // Jan 1's week-start lands in prior year; week 1 starts the next start-day.
        jan1 + chrono::Duration::days((7 - offset) as i64)
    }
}

fn days_until_start(from: Weekday, start: Weekday) -> u32 {
    (weekday_idx(start) + 7 - weekday_idx(from)) % 7
}

fn weekday_idx(w: Weekday) -> u32 {
    match w {
        Weekday::Mon => 0,
        Weekday::Tue => 1,
        Weekday::Wed => 2,
        Weekday::Thu => 3,
        Weekday::Fri => 4,
        Weekday::Sat => 5,
        Weekday::Sun => 6,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn os_week_default_mode_zero() {
        // 2008-02-20 (Wed) → MySQL mode 0 = 7 (ISO week is 8).
        let d = NaiveDate::from_ymd_opt(2008, 2, 20).unwrap();
        assert_eq!(os_week_number(d, 0), 7);
    }

    #[test]
    fn os_week_table_from_test_source() {
        // From CalciteDateTimeFunctionIT.testWeek: each row = (date, mode, expected).
        let cases: &[(NaiveDate, i32, u32)] = &[
            (NaiveDate::from_ymd_opt(2008, 2, 20).unwrap(), 0, 7),
            (NaiveDate::from_ymd_opt(2008, 2, 20).unwrap(), 1, 8),
            (NaiveDate::from_ymd_opt(2008, 12, 31).unwrap(), 1, 53),
            (NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(), 0, 0),
            (NaiveDate::from_ymd_opt(2000, 1, 1).unwrap(), 2, 52),
        ];
        for (d, mode, want) in cases {
            assert_eq!(os_week_number(*d, *mode), *want, "date={d}, mode={mode}");
        }
    }
}
