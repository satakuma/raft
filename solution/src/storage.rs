//! StableStorage implementation.

use crate::{LogEntry, StableStorage};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use tokio::fs::{remove_file, rename, File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;
use uuid::Uuid;

use std::sync::Arc;

#[derive(Serialize, Deserialize)]
pub(crate) struct Log {
    entries: Vec<LogEntry>,
}

impl Log {
    pub(crate) fn empty() -> Log {
        Log {
            entries: Vec::new(),
        }
    }

    pub(crate) fn last_log_index(&self) -> usize {
        self.entries.len() - 1
    }
}

impl Deref for Log {
    type Target = Vec<LogEntry>;
    fn deref(&self) -> &Self::Target {
        &self.entries
    }
}

impl DerefMut for Log {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.entries
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub(crate) struct PersistentState {
    pub current_term: u64,
    pub voted_for: Option<Uuid>,
    pub leader_id: Option<Uuid>,
}

#[derive(Clone)]
pub(crate) struct Storage(Arc<Mutex<Box<dyn StableStorage>>>);

impl Storage {
    pub(crate) fn new(inner: Box<dyn StableStorage>) -> Storage {
        Storage(Arc::new(Mutex::new(inner)))
    }

    async fn put(&self, key: &str, value: &[u8]) {
        self.0.lock().await.put(key, value).await.unwrap();
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.0.lock().await.get(key).await
    }
}

pub(crate) struct Persistent<T> {
    value: T,
    name: String,
    storage: Storage,
}

impl<T> Persistent<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub(crate) async fn new(name: impl AsRef<str>, value: T, storage: &Storage) -> Persistent<T> {
        match storage.get(name.as_ref()).await {
            Some(raw) => {
                let value = bincode::deserialize(&raw).unwrap();
                Persistent {
                    value,
                    name: name.as_ref().to_string(),
                    storage: storage.clone(),
                }
            }
            None => {
                storage
                    .put(name.as_ref(), &bincode::serialize(&value).unwrap())
                    .await;
                Persistent {
                    value,
                    name: name.as_ref().to_string(),
                    storage: storage.clone(),
                }
            }
        }
    }

    async fn save(&self) {
        self.storage
            .put(&self.name, &bincode::serialize(&self.value).unwrap())
            .await;
    }

    pub(crate) async fn update(&mut self, value: T) {
        self.value = value;
        self.save().await;
    }

    pub(crate) async fn update_with<F>(&mut self, f: F)
    where
        F: FnOnce(&mut T),
    {
        f(&mut self.value);
        self.save().await;
    }

    pub(crate) fn mutate(&mut self) -> PersistentGuard<'_, T> {
        PersistentGuard::new(self)
    }
}

impl<T> Deref for Persistent<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

pub(crate) struct PersistentGuard<'a, T> {
    pvalue: &'a mut Persistent<T>,
    saved: bool,
}

impl<'a, T> PersistentGuard<'a, T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn new(pvalue: &mut Persistent<T>) -> PersistentGuard<'_, T> {
        PersistentGuard {
            pvalue,
            saved: false,
        }
    }

    pub(crate) async fn save(mut self) {
        self.pvalue.save().await;
        self.saved = true;
    }
}

impl<'a, T> Drop for PersistentGuard<'a, T> {
    fn drop(&mut self) {
        if !self.saved {
            panic!("unsaved changes at guard drop, fix your code");
        }
    }
}

impl<'a, T> Deref for PersistentGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.pvalue.value
    }
}

impl<'a, T> DerefMut for PersistentGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.pvalue.value
    }
}

const URL_SAFE_ENGINE: base64::engine::fast_portable::FastPortable =
    base64::engine::fast_portable::FastPortable::from(
        &base64::alphabet::URL_SAFE,
        base64::engine::fast_portable::NO_PAD,
    );

/// Storage using POSIX filesystem with atomic operations.
pub(crate) struct PosixStorage {
    path: PathBuf,
}

impl PosixStorage {
    async fn sync_dir(&self) -> io::Result<()> {
        File::open(&self.path).await?.sync_data().await
    }

    fn make_path_pair(&self, filename: &str) -> (PathBuf, PathBuf) {
        let path = self.path.join(filename);
        let tmp_path = path.with_extension("tmp");
        (path, tmp_path)
    }

    fn hash_key(&self, key: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(key.as_bytes());
        base64::encode_engine(hasher.finalize(), &URL_SAFE_ENGINE)
    }

    pub(crate) fn new<P: AsRef<Path>>(path: P) -> PosixStorage {
        PosixStorage {
            path: path.as_ref().to_owned(),
        }
    }

    pub(crate) async fn put_posix(&self, filename: &str, value: &[u8]) -> Result<(), String> {
        let (path, temp_path) = self.make_path_pair(filename);
        let mut temp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(&temp_path)
            .await
            .unwrap();
        temp_file.write_all(value).await.unwrap();
        temp_file.sync_data().await.unwrap();

        rename(&temp_path, &path).await.unwrap();
        self.sync_dir().await.unwrap();

        Ok(())
    }

    pub(crate) async fn get_posix(&self, filename: &str) -> Option<Vec<u8>> {
        let (path, _) = self.make_path_pair(filename);
        let mut file = match File::open(path).await {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return None,
            Err(e) => panic!("filesystem error {:?}", e),
        };

        let mut value = vec![];
        file.read_to_end(&mut value).await.unwrap();
        Some(value)
    }

    #[allow(dead_code)]
    pub(crate) async fn remove_posix(&self, filename: &str) -> bool {
        let (path, _) = self.make_path_pair(filename);
        match remove_file(path).await {
            Ok(_) => {
                self.sync_dir().await.unwrap();
                true
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => panic!("filesystem error {:?}", e),
        }
    }
}

const MAX_KEY_SIZE: usize = 255;

#[async_trait::async_trait]
impl StableStorage for PosixStorage {
    async fn put(&mut self, key: &str, value: &[u8]) -> Result<(), String> {
        if key.len() > MAX_KEY_SIZE {
            return Err("invalid key".into());
        }
        self.put_posix(&self.hash_key(key), value).await
    }

    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.get_posix(&self.hash_key(key)).await
    }
}
