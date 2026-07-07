/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Minimal HyperLogLog copy from DataFusion (Apache-2.0). Wire-compatible with DF54's Binary state format.
//! TODO: Remove this file when upgrading to DataFusion 55+ (HyperLogLog becomes pub).

use datafusion::common::{internal_datafusion_err, DataFusionError, Result};
use datafusion::scalar::ScalarValue;
use std::hash::Hash;
use std::marker::PhantomData;

const HLL_P: usize = 14;
const HLL_Q: usize = 64 - HLL_P;
const NUM_REGISTERS: usize = 1 << HLL_P;
const HLL_P_MASK: u64 = (NUM_REGISTERS as u64) - 1;

/// Same seed as DF54's internal HLL_HASH_STATE.
pub const HLL_HASH_STATE: foldhash::quality::FixedState =
    foldhash::quality::FixedState::with_seed(0);

#[derive(Clone, Debug)]
pub struct HyperLogLog<T: Hash + ?Sized> {
    registers: [u8; NUM_REGISTERS],
    _phantom: PhantomData<T>,
}

impl<T: Hash + ?Sized> HyperLogLog<T> {
    pub fn new() -> Self {
        Self {
            registers: [0; NUM_REGISTERS],
            _phantom: PhantomData,
        }
    }

    #[inline]
    pub fn add(&mut self, obj: &T) {
        use std::hash::BuildHasher;
        self.add_hashed(HLL_HASH_STATE.hash_one(obj));
    }

    #[inline]
    pub fn add_hashed(&mut self, hash: u64) {
        let index = (hash & HLL_P_MASK) as usize;
        let p = ((hash >> HLL_P) | (1_u64 << HLL_Q)).trailing_zeros() + 1;
        self.registers[index] = self.registers[index].max(p as u8);
    }

    pub fn merge(&mut self, other: &HyperLogLog<T>) {
        for i in 0..NUM_REGISTERS {
            self.registers[i] = self.registers[i].max(other.registers[i]);
        }
    }

    pub fn count(&self) -> usize {
        let mut histogram = [0u32; HLL_Q + 2];
        for &r in &self.registers {
            histogram[r as usize] += 1;
        }
        let m = NUM_REGISTERS as f64;
        let mut z = m * hll_tau((m - histogram[HLL_Q + 1] as f64) / m);
        for i in histogram[1..=HLL_Q].iter().rev() {
            z += *i as f64;
            z *= 0.5;
        }
        z += m * hll_sigma(histogram[0] as f64 / m);
        (0.5 / 2_f64.ln() * m * m / z).round() as usize
    }

    pub fn to_scalar(&self) -> ScalarValue {
        ScalarValue::Binary(Some(self.registers.to_vec()))
    }
}

impl<T: Hash + ?Sized> TryFrom<&[u8]> for HyperLogLog<T> {
    type Error = DataFusionError;
    fn try_from(v: &[u8]) -> Result<Self> {
        let arr: [u8; NUM_REGISTERS] = v.try_into().map_err(|_| {
            internal_datafusion_err!("Invalid HLL state: expected {} bytes", NUM_REGISTERS)
        })?;
        Ok(Self {
            registers: arr,
            _phantom: PhantomData,
        })
    }
}

#[inline]
fn hll_sigma(x: f64) -> f64 {
    if x == 1. {
        return f64::INFINITY;
    }
    let mut y = 1.0;
    let mut z = x;
    let mut x = x;
    loop {
        x *= x;
        let z_prime = z;
        z += x * y;
        y += y;
        if z_prime == z {
            break;
        }
    }
    z
}

#[inline]
fn hll_tau(x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let mut y = 1.0;
    let mut z = 1.0 - x;
    let mut x = x;
    loop {
        x = x.sqrt();
        let z_prime = z;
        y *= 0.5;
        z -= (1.0 - x).powi(2) * y;
        if z_prime == z {
            break;
        }
    }
    z / 3.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::hash::BuildHasher;

    #[test]
    fn empty_hll_counts_zero() {
        let hll: HyperLogLog<u8> = HyperLogLog::new();
        assert_eq!(hll.count(), 0);
    }

    #[test]
    fn single_value_counts_one() {
        let mut hll: HyperLogLog<u8> = HyperLogLog::new();
        hll.add_hashed(HLL_HASH_STATE.hash_one("hello"));
        assert_eq!(hll.count(), 1);
    }

    #[test]
    fn duplicate_values_count_once() {
        let mut hll: HyperLogLog<u8> = HyperLogLog::new();
        for _ in 0..1000 {
            hll.add_hashed(HLL_HASH_STATE.hash_one("same"));
        }
        assert_eq!(hll.count(), 1);
    }

    #[test]
    fn distinct_values_counted() {
        let mut hll: HyperLogLog<u8> = HyperLogLog::new();
        for i in 0..100 {
            hll.add_hashed(HLL_HASH_STATE.hash_one(&i));
        }
        let count = hll.count();
        assert!(count >= 95 && count <= 105, "expected ~100, got {count}");
    }

    #[test]
    fn merge_produces_union() {
        let mut hll1: HyperLogLog<u8> = HyperLogLog::new();
        let mut hll2: HyperLogLog<u8> = HyperLogLog::new();
        for i in 0..50 {
            hll1.add_hashed(HLL_HASH_STATE.hash_one(&i));
        }
        for i in 50..100 {
            hll2.add_hashed(HLL_HASH_STATE.hash_one(&i));
        }
        hll1.merge(&hll2);
        let count = hll1.count();
        assert!(count >= 95 && count <= 105, "expected ~100, got {count}");
    }

    #[test]
    fn state_roundtrip() {
        let mut hll: HyperLogLog<u8> = HyperLogLog::new();
        for i in 0..50 {
            hll.add_hashed(HLL_HASH_STATE.hash_one(&i));
        }
        let scalar = hll.to_scalar();
        let ScalarValue::Binary(Some(bytes)) = scalar else { panic!("expected Binary") };
        let restored: HyperLogLog<u8> = bytes.as_slice().try_into().unwrap();
        assert_eq!(hll.count(), restored.count());
    }

    #[test]
    fn invalid_state_returns_error() {
        let short_bytes = vec![0u8; 10];
        let result: Result<HyperLogLog<u8>> = short_bytes.as_slice().try_into();
        assert!(result.is_err());
    }
}
