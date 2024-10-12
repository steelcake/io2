use std::alloc::Allocator;

pub struct VecMap<K: PartialEq, V, A: Allocator + Copy> {
    keys: Vec<K, A>,
    values: Vec<V, A>,
}

impl<K: PartialEq, V, A: Allocator + Copy> VecMap<K, V, A> {
    pub fn with_capacity_in(capacity: usize, alloc: A) -> Self {
        Self {
            keys: Vec::with_capacity_in(capacity, alloc),
            values: Vec::with_capacity_in(capacity, alloc),
        }
    }

    pub fn clear(&mut self) {
        self.keys.clear();
        self.values.clear();
    }

    pub fn is_empty(&self) -> bool {
        assert_eq!(self.keys.len(), self.values.len());
        self.keys.is_empty()
    }

    pub fn insert(&mut self, key: K, value: V) -> Option<V> {
        for (i, k) in self.keys.iter().enumerate() {
            if k == &key {
                return Some(std::mem::replace(self.values.get_mut(i).unwrap(), value));
            }
        }

        self.keys.push(key);
        self.values.push(value);

        None
    }

    pub fn get(&self, key: &K) -> Option<&V> {
        for (i, k) in self.keys.iter().enumerate() {
            if k == key {
                return Some(self.values.get(i).unwrap());
            }
        }

        None
    }

    pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
        for (i, k) in self.keys.iter().enumerate() {
            if k == key {
                return Some(self.values.get_mut(i).unwrap());
            }
        }

        None
    }

    pub fn remove(&mut self, key: &K) -> Option<V> {
        for (i, k) in self.keys.iter().enumerate() {
            if k == key {
                self.keys.swap_remove(i);
                return Some(self.values.swap_remove(i));
            }
        }

        None
    }

    pub fn iter(&self) -> std::iter::Zip<std::slice::Iter<'_, K>, std::slice::Iter<'_, V>> {
        self.keys.iter().zip(self.values.iter())
    }

    pub fn iter_mut(
        &mut self,
    ) -> std::iter::Zip<std::slice::IterMut<'_, K>, std::slice::IterMut<'_, V>> {
        self.keys.iter_mut().zip(self.values.iter_mut())
    }

    pub fn iter_keys(&self) -> std::slice::Iter<'_, K> {
        self.keys.iter()
    }
}
