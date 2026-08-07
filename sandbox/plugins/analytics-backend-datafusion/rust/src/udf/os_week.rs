/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! MySQL `WEEK()` / `YEARWEEK()` semantics (modes 0..7), used by PPL's `WEEK` / `WEEK_OF_YEAR`
//! and `YEARWEEK`. DataFusion's `date_part('week', ts)` is ISO-only and disagrees with MySQL's
//! default mode 0 (Sunday-first). Both UDFs share the per-mode week math (`os_week_number`):
//!
//! - `os_week(date [, mode])` → the MySQL week number.
//! - `os_yearweek(date [, mode])` → `year * 100 + week`, with MySQL's year-boundary roll.

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
    ctx.register_udf(ScalarUDF::from(OsYearweekUdf::new()));
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
            other => {
                return plan_err!("os_week: arg 0 expected date/timestamp/string, got {other:?}")
            }
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
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "os_week: arg 0 not coerced to Date32".to_string(),
                )
            })?;
        let mut builder = Int32Builder::with_capacity(n);
        for i in 0..n {
            if dates.is_null(i) {
                builder.append_null();
                continue;
            }
            let mode = match &mode_arg {
                Some(arr) if arr.is_null(i) => 0,
                Some(arr) => arr
                    .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                    .value(i),
                None => 0,
            };
            let days = dates.value(i);
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719_163).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "os_week: invalid Date32 day count {days}"
                ))
            })?;
            builder.append_value(os_week_number(date, mode) as i32);
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

#[derive(Debug)]
pub struct OsYearweekUdf {
    signature: Signature,
}

impl OsYearweekUdf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

udf_identity!(OsYearweekUdf, "os_yearweek");

impl ScalarUDFImpl for OsYearweekUdf {
    fn name(&self) -> &str {
        "os_yearweek"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return plan_err!(
                "os_yearweek expects 1 or 2 arguments, got {}",
                arg_types.len()
            );
        }
        Ok(DataType::Int32)
    }
    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() || arg_types.len() > 2 {
            return plan_err!(
                "os_yearweek expects 1 or 2 arguments, got {}",
                arg_types.len()
            );
        }
        let mut out = Vec::with_capacity(arg_types.len());
        match &arg_types[0] {
            DataType::Date32
            | DataType::Date64
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View => out.push(DataType::Date32),
            other => {
                return plan_err!(
                    "os_yearweek: arg 0 expected date/timestamp/string, got {other:?}"
                )
            }
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
                other => {
                    return plan_err!("os_yearweek: arg 1 expected integer mode, got {other:?}")
                }
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
            .ok_or_else(|| {
                datafusion::error::DataFusionError::Internal(
                    "os_yearweek: arg 0 not coerced to Date32".to_string(),
                )
            })?;
        let mut builder = Int32Builder::with_capacity(n);
        for i in 0..n {
            if dates.is_null(i) {
                builder.append_null();
                continue;
            }
            let mode = match &mode_arg {
                Some(arr) if arr.is_null(i) => 0,
                Some(arr) => arr
                    .as_primitive::<datafusion::arrow::datatypes::Int32Type>()
                    .value(i),
                None => 0,
            };
            let days = dates.value(i);
            let date = NaiveDate::from_num_days_from_ce_opt(days + 719_163).ok_or_else(|| {
                datafusion::error::DataFusionError::Execution(format!(
                    "os_yearweek: invalid Date32 day count {days}"
                ))
            })?;
            builder.append_value(os_yearweek_number(date, mode));
        }
        Ok(ColumnarValue::Array(Arc::new(builder.finish()) as ArrayRef))
    }
}

/// MySQL `YEARWEEK(date, mode)` — `year * 100 + week`. Bumps mode to 2 or 7 when the
/// per-mode week number is 0 so the date is re-counted in the prior year's frame.
fn os_yearweek_number(date: NaiveDate, mode: i32) -> i32 {
    let mode_resolved = if os_week_number(date, mode) != 0 {
        mode
    } else if mode.rem_euclid(8) <= 4 {
        2
    } else {
        7
    };
    let week = os_week_number(date, mode_resolved);
    let year = os_year_number(date, mode_resolved);
    year * 100 + week as i32
}

/// MySQL `WEEK()` modes 0..7. Modes 0/1/4/5 wrap early-January dates to week 0;
/// see https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_week.
fn os_week_number(date: NaiveDate, mode: i32) -> u32 {
    let m = mode.rem_euclid(8) as u32;
    let raw = java_calendar_week_of_year(date, week_params(m));
    if raw > 51 && date.day() < 7 && matches!(m, 0 | 1 | 4 | 5) {
        0
    } else {
        raw
    }
}

/// Year that owns the week. Decrements for early-January dates that belong to the
/// prior year's last week.
fn os_year_number(date: NaiveDate, mode: i32) -> i32 {
    let m = mode.rem_euclid(8) as u32;
    let raw = java_calendar_week_of_year(date, week_params(m));
    if raw > 51 && date.day() < 7 {
        date.year() - 1
    } else {
        date.year()
    }
}

/// `(firstDayOfWeek, minimalDaysInFirstWeek)` for MySQL modes 0..7.
fn week_params(mode: u32) -> (Weekday, u32) {
    let first = if mode % 2 == 0 {
        Weekday::Sun
    } else {
        Weekday::Mon
    };
    let min_days = match mode {
        1 | 3 => 5,
        4 | 6 => 4,
        _ => 7,
    };
    (first, min_days)
}

/// Java `Calendar.WEEK_OF_YEAR` under `(firstDayOfWeek, minimalDaysInFirstWeek)`.
fn java_calendar_week_of_year(date: NaiveDate, (start, min_days): (Weekday, u32)) -> u32 {
    let year = date.year();
    let this_w1 = week1_start(year, start, min_days);
    if date < this_w1 {
        let prev_w1 = week1_start(year - 1, start, min_days);
        let days = (date - prev_w1).num_days();
        (days / 7) as u32 + 1
    } else {
        let next_w1 = week1_start(year + 1, start, min_days);
        if date >= next_w1 {
            1
        } else {
            let days = (date - this_w1).num_days();
            (days / 7) as u32 + 1
        }
    }
}

/// First day of `year`'s week 1: the week containing Jan 1 owns week 1 iff it has
/// at least `min_days` days in `year`, else week 1 starts the next `start`-day.
fn week1_start(year: i32, start: Weekday, min_days: u32) -> NaiveDate {
    let jan1 = NaiveDate::from_ymd_opt(year, 1, 1).unwrap();
    let back = back_offset(jan1.weekday(), start);
    if 7 - back >= min_days {
        jan1 - chrono::Duration::days(back as i64)
    } else {
        jan1 + chrono::Duration::days((7 - back) as i64)
    }
}

/// Days from `from` back to the most recent `start` weekday.
fn back_offset(from: Weekday, start: Weekday) -> u32 {
    (weekday_idx(from) + 7 - weekday_idx(start)) % 7
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

    #[test]
    fn os_yearweek_matches_sql_plugin() {
        // From CalciteDateTimeFunctionIT.testYearWeek: yearweek('2003-10-03')=200339,
        // yearweek('2003-10-03', 3)=200340.
        let d = NaiveDate::from_ymd_opt(2003, 10, 3).unwrap();
        assert_eq!(os_yearweek_number(d, 0), 200339);
        assert_eq!(os_yearweek_number(d, 3), 200340);
    }

    #[test]
    fn os_yearweek_rolls_year_at_boundary() {
        // 2000-01-01 (Sat), mode 0 → week 0 → bumps to mode 2 → prior year's last week (1999-52).
        let d = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        assert_eq!(os_yearweek_number(d, 0), 199952);
    }

    #[test]
    fn os_yearweek_dec_1899_to_jan_1900_boundary() {
        let dec30_1899 = NaiveDate::from_ymd_opt(1899, 12, 30).unwrap();
        let jan1_1900 = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
        assert_eq!(os_yearweek_number(dec30_1899, 0), 189952);
        assert_eq!(os_yearweek_number(dec30_1899, 4), 189952);
        assert_eq!(os_yearweek_number(jan1_1900, 0), 189953);
        assert_eq!(os_yearweek_number(jan1_1900, 4), 190001);
    }

    #[test]
    fn os_yearweek_jan1_1900_all_modes() {
        let d = NaiveDate::from_ymd_opt(1900, 1, 1).unwrap();
        let expected: [(i32, i32); 8] = [
            (0, 189953),
            (1, 190001),
            (2, 189953),
            (3, 190001),
            (4, 190001),
            (5, 190001),
            (6, 190001),
            (7, 190001),
        ];
        for (mode, want) in expected {
            assert_eq!(os_yearweek_number(d, mode), want, "mode={mode}");
        }
    }
}
