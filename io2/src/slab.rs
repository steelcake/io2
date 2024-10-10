use std::alloc::Allocator;

pub struct Slab<T, A: Allocator> {
    elems: Vec<Entry<T>, A>,
    first_free_entry: usize,
    current_generation: u32,
}

impl<T, A: Allocator> Slab<T, A> {
    pub fn with_capacity_in(capacity: usize, alloc: A) -> Self {
        let mut elems = Vec::with_capacity_in(capacity, alloc);

        for i in 0..capacity {
            elems.push(Entry::Free { next_free: i + 1 });
        }

        Self {
            elems,
            first_free_entry: 0,
            current_generation: 0,
        }
    }

    pub fn insert(&mut self, val: T) -> Key {
        let key_idx = self.first_free_entry;
        let entry = match self.elems.get_mut(self.first_free_entry) {
            Some(entry) => entry,
            None => {
                assert_eq!(self.first_free_entry, self.elems.len() + 1);
                let extend_len = self.elems.len();
                self.elems.reserve_exact(extend_len);
                for i in 0..extend_len {
                    self.elems.push(Entry::Free { next_free: i + 1 });
                }
                self.elems.get_mut(self.first_free_entry).unwrap()
            }
        };

        match entry {
            Entry::Free { next_free } => {
                self.first_free_entry = *next_free;
                *entry = Entry::Occupied {
                    generation: self.current_generation,
                    val,
                };
            }
            _ => unimplemented!(),
        }

        Key {
            generation: self.current_generation,
            index: key_idx,
        }
    }

    pub fn get(&self, key: Key) -> Option<&T> {
        match self.elems.get(key.index) {
            Some(entry) => match entry {
                Entry::Occupied { generation, val } => {
                    if *generation > key.generation {
                        None
                    } else {
                        Some(val)
                    }
                }
                Entry::Free { .. } => None,
            },
            None => None,
        }
    }

    pub fn get_mut(&mut self, key: Key) -> Option<&mut T> {
        match self.elems.get_mut(key.index) {
            Some(entry) => match entry {
                Entry::Occupied { generation, val } => {
                    if *generation > key.generation {
                        None
                    } else {
                        Some(val)
                    }
                }
                Entry::Free { .. } => None,
            },
            None => None,
        }
    }

    pub fn remove(&mut self, key: Key) -> Option<T> {
        match self.elems.get_mut(key.index) {
            Some(entry) => match entry {
                Entry::Occupied { generation, .. } => {
                    if *generation > key.generation {
                        None
                    } else {
                        let entry = std::mem::replace(
                            entry,
                            Entry::Free {
                                next_free: self.first_free_entry,
                            },
                        );
                        self.first_free_entry = key.index;
                        self.current_generation = self.current_generation.wrapping_add(1);
                        match entry {
                            Entry::Occupied { val, .. } => Some(val),
                            _ => unreachable!(),
                        }
                    }
                }
                Entry::Free { .. } => None,
            },
            None => None,
        }
    }
}

enum Entry<T> {
    Occupied { generation: u32, val: T },
    Free { next_free: usize },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key {
    index: usize,
    generation: u32,
}

impl TryFrom<Key> for u64 {
    type Error = std::num::TryFromIntError;

    fn try_from(key: Key) -> Result<Self, Self::Error> {
        let index = u32::try_from(key.index)?;

        Ok(index as u64 | (key.generation as u64) << 32)
    }
}

impl TryFrom<u64> for Key {
    type Error = std::num::TryFromIntError;

    fn try_from(val: u64) -> Result<Self, Self::Error> {
        let index = usize::try_from(val as u32)?;
        let generation = (val >> 32) as u32;

        Ok(Key { index, generation })
    }
}
