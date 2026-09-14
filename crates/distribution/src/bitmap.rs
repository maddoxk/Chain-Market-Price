//! # Inverted Roaring-Style Client Bitmap Index
//!
//! Provides ultra-low latency, branchless client subscription indexing.
//! Utilizes hardware POPCNT and trailing zero count (CTZ / TZCNT) for single-cycle dispatch.

/// Default number of 64-bit words (16 * 64 = 1024 clients per subscription shard)
pub const DEFAULT_BITMAP_WORDS: usize = 16;
pub const MAX_CLIENT_CAPACITY: usize = DEFAULT_BITMAP_WORDS * 64;

/// Cache-aligned dense 64-bit client bitset
#[repr(C, align(64))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientBitmap {
    pub words: [u64; DEFAULT_BITMAP_WORDS],
}

impl Default for ClientBitmap {
    #[inline(always)]
    fn default() -> Self {
        Self::new()
    }
}

impl ClientBitmap {
    #[inline(always)]
    pub const fn new() -> Self {
        Self {
            words: [0u64; DEFAULT_BITMAP_WORDS],
        }
    }

    #[inline(always)]
    pub fn insert(&mut self, client_id: u32) -> bool {
        let idx = client_id as usize;
        let word_idx = idx / 64;
        let bit_idx = idx % 64;

        if word_idx < DEFAULT_BITMAP_WORDS {
            let mask = 1u64 << bit_idx;
            let already_set = (self.words[word_idx] & mask) != 0;
            self.words[word_idx] |= mask;
            !already_set
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn remove(&mut self, client_id: u32) -> bool {
        let idx = client_id as usize;
        let word_idx = idx / 64;
        let bit_idx = idx % 64;

        if word_idx < DEFAULT_BITMAP_WORDS {
            let mask = 1u64 << bit_idx;
            let was_set = (self.words[word_idx] & mask) != 0;
            self.words[word_idx] &= !mask;
            was_set
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn contains(&self, client_id: u32) -> bool {
        let idx = client_id as usize;
        let word_idx = idx / 64;
        let bit_idx = idx % 64;

        if word_idx < DEFAULT_BITMAP_WORDS {
            (self.words[word_idx] & (1u64 << bit_idx)) != 0
        } else {
            false
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.words.iter().all(|&w| w == 0)
    }

    #[inline(always)]
    pub fn count(&self) -> usize {
        self.words.iter().map(|&w| w.count_ones() as usize).sum()
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.words = [0u64; DEFAULT_BITMAP_WORDS];
    }

    /// SIMD-friendly bitwise AND (intersection)
    #[inline(always)]
    pub fn intersect(&self, other: &Self) -> Self {
        let mut result = Self::new();
        for i in 0..DEFAULT_BITMAP_WORDS {
            result.words[i] = self.words[i] & other.words[i];
        }
        result
    }

    /// SIMD-friendly bitwise OR (union)
    #[inline(always)]
    pub fn union(&self, other: &Self) -> Self {
        let mut result = Self::new();
        for i in 0..DEFAULT_BITMAP_WORDS {
            result.words[i] = self.words[i] | other.words[i];
        }
        result
    }

    /// Fast iteration through set bits using hardware `trailing_zeros` and `BLSR`
    #[inline(always)]
    pub fn for_each(&self, mut f: impl FnMut(u32)) {
        for (word_idx, &word) in self.words.iter().enumerate() {
            let mut w = word;
            let base_id = (word_idx * 64) as u32;
            while w != 0 {
                let bit = w.trailing_zeros();
                f(base_id + bit);
                w &= w - 1; // Clear lowest set bit
            }
        }
    }

    #[inline(always)]
    pub fn iter(&self) -> ClientBitmapIter<'_> {
        ClientBitmapIter {
            bitmap: self,
            word_idx: 0,
            current_word: self.words[0],
        }
    }
}

pub struct ClientBitmapIter<'a> {
    bitmap: &'a ClientBitmap,
    word_idx: usize,
    current_word: u64,
}

impl<'a> Iterator for ClientBitmapIter<'a> {
    type Item = u32;

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        while self.current_word == 0 {
            self.word_idx += 1;
            if self.word_idx >= DEFAULT_BITMAP_WORDS {
                return None;
            }
            self.current_word = self.bitmap.words[self.word_idx];
        }

        let bit = self.current_word.trailing_zeros();
        let client_id = (self.word_idx * 64) as u32 + bit;
        self.current_word &= self.current_word - 1;
        Some(client_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_bitmap_insert_contains_remove() {
        let mut bitmap = ClientBitmap::new();
        assert!(!bitmap.contains(42));
        assert!(bitmap.is_empty());

        assert!(bitmap.insert(42));
        assert!(bitmap.contains(42));
        assert!(!bitmap.insert(42)); // Already inserted
        assert_eq!(bitmap.count(), 1);

        assert!(bitmap.insert(127));
        assert!(bitmap.contains(127));
        assert_eq!(bitmap.count(), 2);

        assert!(bitmap.remove(42));
        assert!(!bitmap.contains(42));
        assert_eq!(bitmap.count(), 1);
        assert!(!bitmap.remove(42)); // Already removed
    }

    #[test]
    fn test_client_bitmap_iteration() {
        let mut bitmap = ClientBitmap::new();
        bitmap.insert(3);
        bitmap.insert(63);
        bitmap.insert(64);
        bitmap.insert(200);

        let mut collected = Vec::new();
        bitmap.for_each(|id| collected.push(id));
        assert_eq!(collected, vec![3, 63, 64, 200]);

        let iter_collected: Vec<u32> = bitmap.iter().collect();
        assert_eq!(iter_collected, vec![3, 63, 64, 200]);
    }

    #[test]
    fn test_client_bitmap_set_operations() {
        let mut b1 = ClientBitmap::new();
        let mut b2 = ClientBitmap::new();

        b1.insert(1);
        b1.insert(2);
        b2.insert(2);
        b2.insert(3);

        let intersection = b1.intersect(&b2);
        assert!(!intersection.contains(1));
        assert!(intersection.contains(2));
        assert!(!intersection.contains(3));

        let union = b1.union(&b2);
        assert!(union.contains(1));
        assert!(union.contains(2));
        assert!(union.contains(3));
    }
}
