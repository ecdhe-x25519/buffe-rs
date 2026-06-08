#[derive(Debug)]
pub struct RingBuf<const N: usize> {
    pub data: [usize; N],
    pub head: usize,
    pub tail: usize,
    pub full: bool,
}

impl<const N: usize> RingBuf<N> {
    pub fn new() -> Self {
        Self {
            data: [0; N],
            head: 0,
            tail: 0,
            full: false,
        }
    }

    pub fn write(&mut self, data: usize) -> bool {
        if self.full {
            return false;
        }

        self.data[self.head] = data;
        self.head = (self.head + 1) % N;
        if self.head == self.tail {
            self.full = true;
        }

        true
    }

    pub fn read(&mut self) -> Option<usize> {
        if self.is_empty() {
            return None;
        }

        let data: usize = self.data[self.tail];
        self.tail = (self.tail + 1) % N;
        self.full = false;

        Some(data)
    }

    pub fn len(&self) -> usize {
        if self.full {
            N
        } else if self.head >= self.tail {
            self.head - self.tail
        } else {
            self.head + N - self.tail
        }
    }

    pub fn is_empty(&self) -> bool {
        !self.full && self.head == self.tail
    }
}