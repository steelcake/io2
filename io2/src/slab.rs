use std::alloc::Allocator;

pub struct Slab<T, A: Allocator> {
    elems: Vec<Entry<T>, A>,
    first_free_entry: u32,
    current_generation: u32,
}

impl<T, A: Allocator> Slab<T, A> {
    pub fn with_capacity_in(capacity: usize, alloc: A) -> Self {
        let capacity = if capacity > 0 {
            capacity.next_power_of_two()
        } else {
            0
        };

        let mut elems = Vec::with_capacity_in(capacity, alloc);

        for i in 0..u32::try_from(capacity).unwrap() {
            elems.push(Entry::Free { next_free: i + 1 });
        }

        Self {
            elems,
            first_free_entry: 0,
            current_generation: 0,
        }
    }

    pub fn insert(&mut self, val: T) -> Key {
        let key_idx = usize::try_from(self.first_free_entry).unwrap();
        let entry = match self.elems.get_mut(key_idx) {
            Some(entry) => entry,
            None => {
                assert_eq!(key_idx, self.elems.len());
                let initial_len = u32::try_from(self.elems.len()).unwrap();
                let extend_len = initial_len.max(16);
                self.elems.reserve(usize::try_from(extend_len).unwrap());
                for i in initial_len..initial_len + extend_len {
                    self.elems.push(Entry::Free { next_free: i + 1 });
                }
                self.elems.get_mut(key_idx).unwrap()
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
            _ => unreachable!(),
        }

        Key {
            generation: self.current_generation,
            index: u32::try_from(key_idx).unwrap(),
        }
    }

    pub fn get(&self, key: Key) -> Option<&T> {
        match self.elems.get(usize::try_from(key.index).unwrap()) {
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
        match self.elems.get_mut(usize::try_from(key.index).unwrap()) {
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
        match self.elems.get_mut(usize::try_from(key.index).unwrap()) {
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
    Free { next_free: u32 },
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Key {
    index: u32,
    generation: u32,
}

impl From<u64> for Key {
    fn from(val: u64) -> Self {
        let index = val as u32;
        let generation = (val >> 32) as u32;

        Self { index, generation }
    }
}

impl From<Key> for u64 {
    fn from(key: Key) -> Self {
        key.index as u64 | (key.generation as u64) << 32
    }
}
