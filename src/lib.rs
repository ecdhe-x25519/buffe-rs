#![no_std]

#[cfg(test)]
#[macro_use]
extern crate std;

mod ringbuf;
pub use ringbuf::{SpscRingBuf, MpscRingBuf, MpmcRingBuf};