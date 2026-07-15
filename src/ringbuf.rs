use core::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicUsize, Ordering},
    marker::PhantomData,
    hint,
    ptr,
};

pub trait ProducerPolicy {}
pub trait ConsumerPolicy {}

pub struct Single;
pub struct Multi;

impl ProducerPolicy for Single {}
impl ProducerPolicy for Multi {}
impl ConsumerPolicy for Single {}
impl ConsumerPolicy for Multi {}

pub struct RingBuf<T, const N: usize, P: ProducerPolicy, C: ConsumerPolicy> {
    data: UnsafeCell<[MaybeUninit<T>; N]>,
    write_head: AtomicUsize,
    write_tail: AtomicUsize,
    read_head: AtomicUsize,
    read_tail: AtomicUsize,
    _p: PhantomData<P>,
    _c: PhantomData<C>,
}

pub type SpscRingBuf<T, const N: usize> = RingBuf<T, N, Single, Single>;
pub type MpscRingBuf<T, const N: usize> = RingBuf<T, N, Multi, Single>;
pub type MpmcRingBuf<T, const N: usize> = RingBuf<T, N, Multi, Multi>;

unsafe impl<T: Send, const N: usize, P: ProducerPolicy, C: ConsumerPolicy> Sync for RingBuf<T, N, P, C> {}

impl<T, const N: usize, P: ProducerPolicy, C: ConsumerPolicy> RingBuf<T, N, P, C> {
    pub const fn new() -> Self {
        Self {
            data: UnsafeCell::new(unsafe { MaybeUninit::uninit().assume_init() }),
            write_head: AtomicUsize::new(0),
            write_tail: AtomicUsize::new(0),
            read_head: AtomicUsize::new(0),
            read_tail: AtomicUsize::new(0),
            _p: PhantomData,
            _c: PhantomData,
        }
    }

    pub fn len(&self) -> usize {
        let write = self.write_tail.load(Ordering::Acquire);
        let read = self.read_tail.load(Ordering::Acquire);
        write.wrapping_sub(read)
    }

    pub fn is_full(&self) -> bool {
        self.len() >= N
    }

    fn is_full_raw(head: usize, tail: usize) -> bool {
        head.wrapping_sub(tail) >= N
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    
    fn is_empty_raw(head: usize, tail: usize) -> bool {
        head == tail
    }

    pub const fn capacity(&self) -> usize {
        N
    }
}

impl<T, const N: usize, C: ConsumerPolicy> RingBuf<T, N, Single, C> {
    pub fn push(&self, item: T) -> Result<(), T> {
        let head = self.write_head.load(Ordering::Acquire);
        let tail = self.read_tail.load(Ordering::Acquire);

        if Self::is_full_raw(head, tail) {
            return Err(item);
        }

        let next = head.wrapping_add(1);

        unsafe {
            let ptr = (*self.data.get()).as_mut_ptr();
            ptr::write(ptr.add(head % N), MaybeUninit::new(item));
        }

        self.write_head.store(next, Ordering::Release);
        self.write_tail.store(next, Ordering::Release);
        Ok(())
    }
}

impl<T, const N: usize, C: ConsumerPolicy> RingBuf<T, N, Multi, C> {
    pub fn push(&self, item: T) -> Result<(), T> {
        loop {
            let head = self.write_head.load(Ordering::Acquire);
            let tail = self.read_tail.load(Ordering::Acquire);

            if Self::is_full_raw(head, tail) {
                return Err(item);
            }

            let next = head.wrapping_add(1);

            if self.write_head
                .compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                unsafe {
                    let ptr = (*self.data.get()).as_mut_ptr();
                    ptr::write(ptr.add(head % N), MaybeUninit::new(item));
                }

                while self.write_tail.load(Ordering::Acquire) != head {
                    hint::spin_loop();
                }

                self.write_tail.store(next, Ordering::Release);
                return Ok(());
            }
            hint::spin_loop();
        }
    }
}

impl<T, const N: usize, P: ProducerPolicy> RingBuf<T, N, P, Single> {
    pub fn pop(&self) -> Option<T> {
        let head = self.read_head.load(Ordering::Acquire);
        let tail = self.write_tail.load(Ordering::Acquire);

        if Self::is_empty_raw(head, tail) {
            return None;
        }

        let item = unsafe {
            let ptr = (*self.data.get()).as_mut_ptr();
            MaybeUninit::assume_init(ptr::read(ptr.add(head % N)))
        };

        self.read_head.store(head.wrapping_add(1), Ordering::Release);
        self.read_tail.store(head.wrapping_add(1), Ordering::Release);
        
        Some(item)
    }
}

impl<T, const N: usize, P: ProducerPolicy> RingBuf<T, N, P, Multi> {
    pub fn pop(&self) -> Option<T> {
        loop {
            let head = self.read_head.load(Ordering::Acquire);
            let tail = self.write_tail.load(Ordering::Acquire);

            if Self::is_empty_raw(head, tail) {
                return None;
            }

            let next = head.wrapping_add(1);

            if self.read_head
            .compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            {
                let item = unsafe {
                    let ptr = (*self.data.get()).as_mut_ptr();
                    MaybeUninit::assume_init(ptr::read(ptr.add(head % N)))
                };

                while self.read_tail.load(Ordering::Acquire) != head {
                    hint::spin_loop();
                }
                
                self.read_tail.store(next, Ordering::Release);
                return Some(item);
            }
            
            hint::spin_loop();
        }
    }
}

impl<T, const N: usize, P: ProducerPolicy, C: ConsumerPolicy> Drop for RingBuf<T, N, P, C> {
    fn drop(&mut self) {
        unsafe {
            let write = self.write_tail.load(Ordering::Relaxed);
            let mut read = self.read_tail.load(Ordering::Relaxed);
            
            while read != write {
                let index = read % N;
                let ptr = (*self.data.get()).as_mut_ptr();
                let item = ptr::read(ptr.add(index));
                drop(MaybeUninit::assume_init(item));
                read = read.wrapping_add(1);
            }
        }
    }
}

#[cfg(test)]
mod ringbuf_tests {
    use super::*;

    #[test]
    fn push_pop() {
        let buf: SpscRingBuf<i32, 4> = SpscRingBuf::new();
        assert!(buf.is_empty());
        assert_eq!(buf.len(), 0);
        assert_eq!(buf.pop(), None);

        buf.push(1).unwrap();
        buf.push(2).unwrap();
        buf.push(3).unwrap();
        assert_eq!(buf.len(), 3);
        assert!(!buf.is_full());

        assert_eq!(buf.pop(), Some(1));
        assert_eq!(buf.pop(), Some(2));
        assert_eq!(buf.pop(), Some(3));
        assert_eq!(buf.pop(), None);
        assert!(buf.is_empty());
    }

    #[test]
    fn full() {
        let buf: SpscRingBuf<i32, 2> = SpscRingBuf::new();
        buf.push(1).unwrap();
        buf.push(2).unwrap();
        assert!(buf.is_full());
        assert_eq!(buf.push(3), Err(3));
        buf.pop().unwrap();
        buf.push(3).unwrap();
        assert_eq!(buf.pop(), Some(2));
        assert_eq!(buf.pop(), Some(3));
    }

    #[test]
    fn wrapping() {
        let buf: SpscRingBuf<i32, 4> = SpscRingBuf::new();
        for cycle in 0..100 {
            for i in 0..4 {
                buf.push(cycle * 4 + i).unwrap();
            }
            for i in 0..4 {
                assert_eq!(buf.pop(), Some(cycle * 4 + i));
            }
        }
    }

    #[test]
    fn spsc_two_threads() {
        let buf: &'static SpscRingBuf<i32, 16> = {
            static BUF: SpscRingBuf<i32, 16> = SpscRingBuf::new();
            &BUF
        };

        let producer = std::thread::spawn(move || {
            for i in 0..1000 {
                loop {
                    if buf.push(i).is_ok() {
                        break;
                    }
                    std::thread::yield_now();
                }
            }
        });

        let consumer = std::thread::spawn(move || {
            let mut sum = 0;
            for _ in 0..1000 {
                loop {
                    if let Some(val) = buf.pop() {
                        sum += val;
                        break;
                    }
                    std::thread::yield_now();
                }
            }
            sum
        });

        producer.join().unwrap();
        assert_eq!(consumer.join().unwrap(), 999 * 1000 / 2);
    }

    #[test]
    fn mpsc_four_producers() {
        static BUF: MpscRingBuf<i32, 32> = MpscRingBuf::new();

        let mut handles = vec![];
        
        for t in 0..4 {
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    loop {
                        let item = t * 100 + i;
                        dbg!(item);
                        match BUF.push(item) {
                            Ok(()) => break,
                            Err(_) => std::thread::yield_now(),
                        }
                    }
                }
            }));
        }

        let consumer = std::thread::spawn(move || {
            let mut received = vec![0usize; 400];
            let mut collected = 0;
            dbg!(collected);
            
            while collected < 400 {
                match BUF.pop() {
                    Some(val) => {
                        received[val as usize] += 1;
                        collected += 1;
                    }
                    None => std::thread::yield_now(),
                }
            }
            received
        });

        for h in handles {
            h.join().unwrap();
        }
        
        let received = consumer.join().unwrap();
        
        assert_eq!(received.iter().sum::<usize>(), 400);
        for (i, &count) in received.iter().enumerate() {
            assert_eq!(count, 1, "Value {} received {} times", i, count);
        }
    }

    #[test]
    fn mpmc_four_producers_four_consumers() {
        let buf: &'static MpmcRingBuf<i32, 32> = {
            static BUF: MpmcRingBuf<i32, 32> = MpmcRingBuf::new();
            &BUF
        };

        static PRODUCED: AtomicUsize = AtomicUsize::new(0);
        static CONSUMED: AtomicUsize = AtomicUsize::new(0);

        let mut handles = vec![];

        for t in 0..4 {
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    loop {
                        if buf.push(t * 100 + i).is_ok() {
                            PRODUCED.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                        std::thread::yield_now();
                    }
                }
            }));
        }

        for _ in 0..4 {
            handles.push(std::thread::spawn(move || loop {
                if CONSUMED.load(Ordering::Relaxed) >= 400 {
                    break;
                }
                if buf.pop().is_some() {
                    CONSUMED.fetch_add(1, Ordering::Relaxed);
                } else {
                    std::thread::yield_now();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(PRODUCED.load(Ordering::Relaxed), 400);
        assert_eq!(CONSUMED.load(Ordering::Relaxed), 400);
    }

    #[test]
    fn drop_counter() {
        use core::sync::atomic::{AtomicUsize, Ordering};

        static DROPS: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug)]
        struct Item;
        impl Drop for Item {
            fn drop(&mut self) {
                DROPS.fetch_add(1, Ordering::Relaxed);
            }
        }

        {
            let buf: SpscRingBuf<Item, 4> = SpscRingBuf::new();
            buf.push(Item).unwrap();
            buf.push(Item).unwrap();
            let item = buf.pop().unwrap();
            drop(item);
            assert_eq!(DROPS.load(Ordering::Relaxed), 1);
        }
        assert_eq!(DROPS.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn zst() {
        let buf: SpscRingBuf<(), 4> = SpscRingBuf::new();
        buf.push(()).unwrap();
        buf.push(()).unwrap();
        assert_eq!(buf.len(), 2);
        assert_eq!(buf.pop(), Some(()));
        assert_eq!(buf.pop(), Some(()));
        assert_eq!(buf.pop(), None);
    }

    #[test]
    fn capacity() {
        assert_eq!(SpscRingBuf::<i32, 16>::new().capacity(), 16);
        assert_eq!(MpscRingBuf::<u64, 256>::new().capacity(), 256);
        assert_eq!(MpmcRingBuf::<u8, 8>::new().capacity(), 8);
    }
}