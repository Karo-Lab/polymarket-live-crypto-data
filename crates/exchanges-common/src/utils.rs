use std::{fmt::Debug, hash::Hash, sync::Arc};

use dashmap::DashMap;

use crate::traits::Cache;

pub struct InMemoryStore<K, V> {
    data: DashMap<K, V>,
}

impl<K, V> Debug for InMemoryStore<K, V>
where
    K: Debug,
    V: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InMemoryStore")
            .field("data", &"...")
            .finish_non_exhaustive()
    }
}

impl<K, V> InMemoryStore<K, V>
where
    K: Eq + Hash + Send + Sync + Clone,
    V: Send + Sync + Clone,
{
    #[must_use]
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            data: DashMap::<K, V>::new(),
        })
    }
}

impl<K, V> Cache<K, V> for InMemoryStore<K, V>
where
    K: Eq + Hash + Send + Sync + Clone + Debug,
    V: Send + Sync + Clone + Debug,
{
    /// Return a Cloned value from the map asynchronously
    async fn get<Q>(&self, k: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + Sync + ?Sized,
    {
        self.try_get(k)
    }

    /// Return a Cloned value from the map.
    fn try_get<Q>(&self, k: &Q) -> Option<V>
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + Sync + ?Sized,
    {
        self.data.get(k).map(|r| r.value().clone())
    }

    async fn invalidate<Q>(&self, k: &Q)
    where
        K: std::borrow::Borrow<Q>,
        Q: Hash + Eq + Sync + ?Sized,
    {
        self.data.remove(k);
    }

    async fn set(&self, k: K, v: V) -> Option<V> {
        self.try_set(k, v)
    }

    fn try_set(&self, k: K, v: V) -> Option<V> {
        self.data.insert(k, v)
    }
}
