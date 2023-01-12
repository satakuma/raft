//! StableStorage implementation.

use crate::StableStorage;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use tokio::fs::{remove_file, rename, File, OpenOptions};
use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

type Storage = Rc<RefCell<Box<dyn StableStorage>>>;

pub(crate) struct Persistent<T> {
    value: T,
    name: String,
    storage: Storage,
}

impl<T> Persistent<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    pub async fn new(name: impl AsRef<str>, value: T, storage: &Storage) -> Persistent<T> {
        match storage.borrow().get(name.as_ref()).await {
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
                    .borrow_mut()
                    .put(name.as_ref(), &bincode::serialize(&value).unwrap())
                    .await
                    .unwrap();
                Persistent {
                    value,
                    name: name.as_ref().to_string(),
                    storage: storage.clone(),
                }
            }
        }
    }

    pub async fn set(&mut self, value: T) {
        self.value = value;
        self.storage
            .borrow_mut()
            .put(&self.name, &bincode::serialize(&self.value).unwrap())
            .await
            .unwrap()
    }
}

impl<T> Deref for Persistent<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

pub(crate) struct PersistentVec<T> {
    values: Vec<T>,
    name: String,
    storage: Storage,
}

impl<T> PersistentVec<T>
where
    T: Serialize + for<'de> Deserialize<'de>,
{
    fn item_name(&self, ind: usize) -> String {
        format!("{}_{}", &self.name, ind)
    }

    pub async fn new(name: impl AsRef<str>, storage: &Storage) -> PersistentVec<T> {
        let mut vec = PersistentVec {
            values: Vec::new(),
            name: name.as_ref().to_string(),
            storage: storage.clone(),
        };

        while let Some(raw) = storage.borrow().get(&vec.item_name(vec.values.len())).await {
            vec.values.push(bincode::deserialize(&raw).unwrap());
        }

        vec
    }

    pub async fn set(&mut self, ind: usize, value: T) {
        self.values[ind] = value;
        self.storage
            .borrow_mut()
            .put(
                &self.item_name(ind),
                &bincode::serialize(&self.values[ind]).unwrap(),
            )
            .await
            .unwrap()
    }
}

impl<T> Deref for PersistentVec<T> {
    type Target = Vec<T>;
    fn deref(&self) -> &Self::Target {
        &self.values
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
