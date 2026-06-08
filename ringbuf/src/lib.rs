#![cfg_attr(not(feature = "std"), no_std)]

pub mod ringbuf;
pub use ringbuf::RingBuf;