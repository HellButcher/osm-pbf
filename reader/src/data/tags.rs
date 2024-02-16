use std::slice::Iter;
use std::{iter::FusedIterator, ops::Deref};

#[derive(Copy, Clone)]
pub(super) struct TagFields<'l>(pub &'l [u32], pub &'l [u32]);

#[derive(Copy, Clone)]
pub(super) enum NodeTagFields<'l> {
    Normal(&'l [u32], &'l [u32]),
    Dense(&'l [i32]),
}

#[derive(Clone)]
enum TagIterFields<'l> {
    Normal(Iter<'l, u32>, Iter<'l, u32>),
    Dense(Iter<'l, i32>),
}

pub struct Tags<'l> {
    strings: &'l [String],
    iters: TagIterFields<'l>,
}

impl<'l> TagFields<'l> {
    #[inline]
    pub fn iter_with_strings(self, strings: &'l [String]) -> Tags<'l> {
        Tags {
            strings,
            iters: TagIterFields::Normal(self.0.iter(), self.1.iter()),
        }
    }
}

impl<'l> NodeTagFields<'l> {
    #[inline]
    pub fn iter_with_strings(self, strings: &'l [String]) -> Tags<'l> {
        Tags {
            strings,
            iters: match self {
                NodeTagFields::Normal(keys, values) => {
                    TagIterFields::Normal(keys.iter(), values.iter())
                }
                NodeTagFields::Dense(key_values) => TagIterFields::Dense(key_values.iter()),
            },
        }
    }
}

impl<'l> Iterator for Tags<'l> {
    type Item = (&'l str, &'l str);
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let key_index;
        let value_index;
        match self.iters {
            TagIterFields::Normal(ref mut keys, ref mut values) => {
                key_index = keys.next().copied()? as usize;
                value_index = values.next().copied()? as usize;
            }
            TagIterFields::Dense(ref mut key_values) => {
                key_index = key_values.next().copied()? as usize;
                value_index = key_values.next().copied()? as usize;
            }
        }
        let key = self.strings.get(key_index).map(Deref::deref).unwrap_or("");
        let value = self
            .strings
            .get(value_index)
            .map(Deref::deref)
            .unwrap_or("");
        Some((key, value))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        match self.iters {
            TagIterFields::Normal(ref keys, ref values) => {
                let (keys_lower, keys_upper) = keys.size_hint();
                let (values_lower, values_upper) = values.size_hint();
                (
                    keys_lower.min(values_lower),
                    keys_upper.zip(values_upper).map(|(k, v)| k.min(v)),
                )
            }
            TagIterFields::Dense(ref key_values) => {
                let (lower, upper) = key_values.size_hint();
                (lower / 2, upper.map(|l| l / 2))
            }
        }
    }
}
impl<'l> FusedIterator for Tags<'l> {}
