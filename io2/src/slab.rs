// Taken from https://raw.githubusercontent.com/tokio-rs/slab/refs/heads/master/src/lib.rs
// TODO: remove this and do a simple implementation that works with allocators

extern crate std;
extern crate std as alloc;

use core::{mem, ops};
use std::alloc::Allocator;

pub struct Slab<T, A: Allocator> {
    entries: Vec<Entry<T>, A>,
    len: usize,
    next: usize,
}

impl<T, A: Allocator + Clone> Clone for Slab<T, A>
where
    T: Clone,
{
    fn clone(&self) -> Self {
        Self {
            entries: self.entries.clone(),
            len: self.len,
            next: self.next,
        }
    }

    fn clone_from(&mut self, source: &Self) {
        self.entries.clone_from(&source.entries);
        self.len = source.len;
        self.next = source.next;
    }
}

pub struct VacantEntry<'a, T, A: Allocator> {
    slab: &'a mut Slab<T, A>,
    key: usize,
}

#[derive(Clone)]
enum Entry<T> {
    Vacant(usize),
    Occupied(T),
}

impl<T, A: Allocator> Slab<T, A> {
    pub const fn new_in(alloc: A) -> Self {
        Self {
            entries: Vec::new_in(alloc),
            next: 0,
            len: 0,
        }
    }

    pub fn with_capacity_in(capacity: usize, alloc: A) -> Slab<T, A> {
        Slab {
            entries: Vec::with_capacity_in(capacity, alloc),
            next: 0,
            len: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    pub fn reserve(&mut self, additional: usize) {
        if self.capacity() - self.len >= additional {
            return;
        }
        let need_add = additional - (self.entries.len() - self.len);
        self.entries.reserve(need_add);
    }

    pub fn reserve_exact(&mut self, additional: usize) {
        if self.capacity() - self.len >= additional {
            return;
        }
        let need_add = additional - (self.entries.len() - self.len);
        self.entries.reserve_exact(need_add);
    }

    pub fn shrink_to_fit(&mut self) {
        let len_before = self.entries.len();
        while let Some(&Entry::Vacant(_)) = self.entries.last() {
            self.entries.pop();
        }

        if self.entries.len() != len_before {
            self.recreate_vacant_list();
        }

        self.entries.shrink_to_fit();
    }

    fn recreate_vacant_list(&mut self) {
        self.next = self.entries.len();
        let mut remaining_vacant = self.entries.len() - self.len;
        if remaining_vacant == 0 {
            return;
        }

        for (i, entry) in self.entries.iter_mut().enumerate().rev() {
            if let Entry::Vacant(ref mut next) = *entry {
                *next = self.next;
                self.next = i;
                remaining_vacant -= 1;
                if remaining_vacant == 0 {
                    break;
                }
            }
        }
    }

    pub fn compact<F>(&mut self, mut rekey: F)
    where
        F: FnMut(&mut T, usize, usize) -> bool,
    {
        // If the closure unwinds, we need to restore a valid list of vacant entries
        struct CleanupGuard<'a, T, A: Allocator> {
            slab: &'a mut Slab<T, A>,
            decrement: bool,
        }
        impl<T, A: Allocator> Drop for CleanupGuard<'_, T, A> {
            fn drop(&mut self) {
                if self.decrement {
                    // Value was popped and not pushed back on
                    self.slab.len -= 1;
                }
                self.slab.recreate_vacant_list();
            }
        }
        let mut guard = CleanupGuard {
            slab: self,
            decrement: true,
        };

        let mut occupied_until = 0;
        // While there are vacant entries
        while guard.slab.entries.len() > guard.slab.len {
            // Find a value that needs to be moved,
            // by popping entries until we find an occupied one.
            // (entries cannot be empty because 0 is not greater than anything)
            if let Some(Entry::Occupied(mut value)) = guard.slab.entries.pop() {
                // Found one, now find a vacant entry to move it to
                while let Some(&Entry::Occupied(_)) = guard.slab.entries.get(occupied_until) {
                    occupied_until += 1;
                }
                // Let the caller try to update references to the key
                if !rekey(&mut value, guard.slab.entries.len(), occupied_until) {
                    // Changing the key failed, so push the entry back on at its old index.
                    guard.slab.entries.push(Entry::Occupied(value));
                    guard.decrement = false;
                    guard.slab.entries.shrink_to_fit();
                    return;
                    // Guard drop handles cleanup
                }
                // Put the value in its new spot
                guard.slab.entries[occupied_until] = Entry::Occupied(value);
                // ... and mark it as occupied (this is optional)
                occupied_until += 1;
            }
        }
        guard.slab.next = guard.slab.len;
        guard.slab.entries.shrink_to_fit();
        // Normal cleanup is not necessary
        mem::forget(guard);
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.len = 0;
        self.next = 0;
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn get(&self, key: usize) -> Option<&T> {
        match self.entries.get(key) {
            Some(Entry::Occupied(val)) => Some(val),
            _ => None,
        }
    }

    pub fn get_mut(&mut self, key: usize) -> Option<&mut T> {
        match self.entries.get_mut(key) {
            Some(&mut Entry::Occupied(ref mut val)) => Some(val),
            _ => None,
        }
    }

    pub fn get2_mut(&mut self, key1: usize, key2: usize) -> Option<(&mut T, &mut T)> {
        assert!(key1 != key2);

        let (entry1, entry2);

        if key1 > key2 {
            let (slice1, slice2) = self.entries.split_at_mut(key1);
            entry1 = slice2.get_mut(0);
            entry2 = slice1.get_mut(key2);
        } else {
            let (slice1, slice2) = self.entries.split_at_mut(key2);
            entry1 = slice1.get_mut(key1);
            entry2 = slice2.get_mut(0);
        }

        match (entry1, entry2) {
            (
                Some(&mut Entry::Occupied(ref mut val1)),
                Some(&mut Entry::Occupied(ref mut val2)),
            ) => Some((val1, val2)),
            _ => None,
        }
    }

    pub unsafe fn get_unchecked(&self, key: usize) -> &T {
        match *self.entries.get_unchecked(key) {
            Entry::Occupied(ref val) => val,
            _ => unreachable!(),
        }
    }

    pub unsafe fn get_unchecked_mut(&mut self, key: usize) -> &mut T {
        match *self.entries.get_unchecked_mut(key) {
            Entry::Occupied(ref mut val) => val,
            _ => unreachable!(),
        }
    }

    pub unsafe fn get2_unchecked_mut(&mut self, key1: usize, key2: usize) -> (&mut T, &mut T) {
        debug_assert_ne!(key1, key2);
        let ptr = self.entries.as_mut_ptr();
        let ptr1 = ptr.add(key1);
        let ptr2 = ptr.add(key2);
        match (&mut *ptr1, &mut *ptr2) {
            (&mut Entry::Occupied(ref mut val1), &mut Entry::Occupied(ref mut val2)) => {
                (val1, val2)
            }
            _ => unreachable!(),
        }
    }

    pub fn key_of(&self, present_element: &T) -> usize {
        let element_ptr = present_element as *const T as usize;
        let base_ptr = self.entries.as_ptr() as usize;
        // Use wrapping subtraction in case the reference is bad
        let byte_offset = element_ptr.wrapping_sub(base_ptr);
        // The division rounds away any offset of T inside Entry
        // The size of Entry<T> is never zero even if T is due to Vacant(usize)
        let key = byte_offset / mem::size_of::<Entry<T>>();
        // Prevent returning unspecified (but out of bounds) values
        if key >= self.entries.len() {
            panic!("The reference points to a value outside this slab");
        }
        // The reference cannot point to a vacant entry, because then it would not be valid
        key
    }

    pub fn insert(&mut self, val: T) -> usize {
        let key = self.next;

        self.insert_at(key, val);

        key
    }

    pub fn vacant_key(&self) -> usize {
        self.next
    }

    pub fn vacant_entry(&mut self) -> VacantEntry<'_, T, A> {
        VacantEntry {
            key: self.next,
            slab: self,
        }
    }

    fn insert_at(&mut self, key: usize, val: T) {
        self.len += 1;

        if key == self.entries.len() {
            self.entries.push(Entry::Occupied(val));
            self.next = key + 1;
        } else {
            self.next = match self.entries.get(key) {
                Some(&Entry::Vacant(next)) => next,
                _ => unreachable!(),
            };
            self.entries[key] = Entry::Occupied(val);
        }
    }

    pub fn try_remove(&mut self, key: usize) -> Option<T> {
        if let Some(entry) = self.entries.get_mut(key) {
            // Swap the entry at the provided value
            let prev = mem::replace(entry, Entry::Vacant(self.next));

            match prev {
                Entry::Occupied(val) => {
                    self.len -= 1;
                    self.next = key;
                    return val.into();
                }
                _ => {
                    // Woops, the entry is actually vacant, restore the state
                    *entry = prev;
                }
            }
        }
        None
    }

    pub fn remove(&mut self, key: usize) -> T {
        self.try_remove(key).expect("invalid key")
    }

    pub fn contains(&self, key: usize) -> bool {
        matches!(self.entries.get(key), Some(&Entry::Occupied(_)))
    }

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(usize, &mut T) -> bool,
    {
        for i in 0..self.entries.len() {
            let keep = match self.entries[i] {
                Entry::Occupied(ref mut v) => f(i, v),
                _ => true,
            };

            if !keep {
                self.remove(i);
            }
        }
    }
}

impl<T, A: Allocator> ops::Index<usize> for Slab<T, A> {
    type Output = T;

    fn index(&self, key: usize) -> &T {
        match self.entries.get(key) {
            Some(Entry::Occupied(v)) => v,
            _ => panic!("invalid key"),
        }
    }
}

impl<T, A: Allocator> ops::IndexMut<usize> for Slab<T, A> {
    fn index_mut(&mut self, key: usize) -> &mut T {
        match self.entries.get_mut(key) {
            Some(&mut Entry::Occupied(ref mut v)) => v,
            _ => panic!("invalid key"),
        }
    }
}

// ===== VacantEntry =====

impl<'a, T, A: Allocator> VacantEntry<'a, T, A> {
    pub fn insert(self, val: T) -> &'a mut T {
        self.slab.insert_at(self.key, val);

        match self.slab.entries.get_mut(self.key) {
            Some(&mut Entry::Occupied(ref mut v)) => v,
            _ => unreachable!(),
        }
    }

    pub fn key(&self) -> usize {
        self.key
    }
}
