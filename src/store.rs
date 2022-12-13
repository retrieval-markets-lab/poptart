use ahash::AHashSet;
use anyhow::{anyhow, bail, Context, Result};
use async_trait::async_trait;
use bytecheck::CheckBytes;
use bytes::{Bytes, BytesMut};
use cid::multihash::Multihash;
use cid::Cid;
use iroh_bitswap::{Block, Store as BitswapStore};
use rkyv::{with::AsBox, Archive, Deserialize, Serialize};
/// Ipld Store implementation based on Iroh's RockDB config
/// without the RPC.
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamily, DBPinnableSlice, Direction, IteratorMode, Options,
    WriteBatch, DB as RocksDb,
};
use smallvec::SmallVec;
use std::path::PathBuf;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::available_parallelism;
use tokio::task;
use tork::Store as TorkStore;

/// Creates the default rocksdb options
/// based on Iroh's implementation.
fn default_options() -> (Options, Cache) {
    let mut opts = Options::default();
    opts.set_write_buffer_size(512 * 1024 * 1024);
    opts.optimize_for_point_lookup(64 * 1024 * 1024);
    let par = (available_parallelism().map(|s| s.get()).unwrap_or(2) / 4).min(2);
    opts.increase_parallelism(par as _);
    opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
    opts.set_blob_compression_type(rocksdb::DBCompressionType::Lz4);
    opts.set_bytes_per_sync(1_048_576);
    opts.set_blob_file_size(512 * 1024 * 1024);

    let cache = Cache::new_lru_cache(128 * 1024 * 1024).unwrap();
    let mut bopts = BlockBasedOptions::default();
    // all our data is longer lived, so ribbon filters make sense
    bopts.set_ribbon_filter(10.0);
    bopts.set_block_cache(&cache);
    bopts.set_block_size(6 * 1024);
    bopts.set_cache_index_and_filter_blocks(true);
    bopts.set_pin_l0_filter_and_index_blocks_in_cache(true);
    opts.set_block_based_table_factory(&bopts);

    (opts, cache)
}

fn default_blob_opts() -> Options {
    let mut opts = Options::default();
    opts.set_enable_blob_files(true);
    opts.set_min_blob_size(5 * 1024);

    opts
}

/// The key used in CF_ID_V0
///
/// The multihash followed by the be encoded code. This allows both looking up an id by multihash and code (aka Cid),
/// and looking up all codes and ids for a multihash, for the rare case that there are mulitple cids with the same
/// multihash but different codes.
fn id_key(cid: &Cid) -> SmallVec<[u8; 64]> {
    let mut key = SmallVec::new();
    cid.hash().write(&mut key).unwrap();
    key.extend_from_slice(&cid.codec().to_be_bytes());
    key
}

#[derive(Clone)]
pub struct RockStore {
    inner: Arc<InnerStore>,
}

impl std::fmt::Debug for RockStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RockStore").finish()
    }
}

struct InnerStore {
    content: RocksDb,
    next_id: RwLock<u64>,
    _cache: Cache,
}

#[async_trait]
impl BitswapStore for RockStore {
    async fn get_size(&self, cid: &Cid) -> anyhow::Result<usize> {
        self.get_size(cid)?.ok_or_else(|| anyhow!("not found"))
    }

    async fn get(&self, cid: &Cid) -> anyhow::Result<Block> {
        let slice = self
            .read_store()?
            .get(cid)?
            .ok_or_else(|| anyhow!("not found"))?;
        let bytes = BytesMut::from(&slice[..]).freeze();
        Ok(Block::new(bytes, *cid))
    }

    async fn has(&self, cid: &Cid) -> anyhow::Result<bool> {
        self.has(cid)
    }
}

impl TorkStore for RockStore {
    fn get(&self, cid: &Cid) -> anyhow::Result<Bytes> {
        let slice = self
            .read_store()?
            .get(cid)?
            .ok_or_else(|| anyhow!("not found"))?;
        let bytes = BytesMut::from(&slice[..]).freeze();
        Ok(bytes)
    }

    fn put<T: AsRef<[u8]>, L: IntoIterator<Item = Cid>>(
        &self,
        cid: Cid,
        bytes: T,
        links: L,
    ) -> Result<()> {
        self.put(cid, bytes, links)
    }
}

impl RockStore {
    /// Creates a new database.
    pub async fn create(path: PathBuf) -> Result<Self> {
        let (mut options, cache) = default_options();
        options.create_if_missing(true);

        let db = task::spawn_blocking(move || -> Result<_> {
            let mut db = RocksDb::open(&options, path)?;
            {
                let opts = default_blob_opts();
                db.create_cf(CF_BLOBS_V0, &opts)?;
            }
            {
                let opts = Options::default();
                db.create_cf(CF_METADATA_V0, &opts)?;
            }
            {
                let opts = Options::default();
                db.create_cf(CF_GRAPH_V0, &opts)?;
            }
            {
                let opts = Options::default();
                db.create_cf(CF_ID_V0, &opts)?;
            }

            Ok(db)
        })
        .await??;

        Ok(RockStore {
            inner: Arc::new(InnerStore {
                content: db,
                next_id: 1.into(),
                _cache: cache,
            }),
        })
    }

    pub async fn open(path: PathBuf) -> Result<Self> {
        let (mut options, cache) = default_options();
        options.create_if_missing(false);

        let (db, next_id) = task::spawn_blocking(move || -> Result<_> {
            let db = RocksDb::open_cf(
                &options,
                path,
                [CF_BLOBS_V0, CF_METADATA_V0, CF_GRAPH_V0, CF_ID_V0],
            )?;

            // read last inserted id
            let next_id = {
                let cf_meta = db
                    .cf_handle(CF_METADATA_V0)
                    .ok_or_else(|| anyhow!("missing column family: metadata"))?;

                let mut iter = db.full_iterator_cf(&cf_meta, IteratorMode::End);
                let last_id = iter
                    .next()
                    .and_then(|r| r.ok())
                    .and_then(|(key, _)| key[..8].try_into().ok())
                    .map(u64::from_be_bytes)
                    .unwrap_or_default();

                last_id + 1
            };

            Ok((db, next_id))
        })
        .await??;

        Ok(RockStore {
            inner: Arc::new(InnerStore {
                content: db,
                next_id: next_id.into(),
                _cache: cache,
            }),
        })
    }

    pub fn put<T: AsRef<[u8]>, L>(&self, cid: Cid, blob: T, links: L) -> Result<()>
    where
        L: IntoIterator<Item = Cid>,
    {
        self.write_store()?.put(cid, blob, links)
    }

    pub fn put_many(&self, blocks: impl IntoIterator<Item = (Cid, Bytes, Vec<Cid>)>) -> Result<()> {
        self.write_store()?.put_many(blocks)
    }

    pub fn get_blob_by_hash(&self, hash: &Multihash) -> Result<Option<DBPinnableSlice<'_>>> {
        self.read_store()?.get_blob_by_hash(hash)
    }

    pub fn has_blob_for_hash(&self, hash: &Multihash) -> Result<bool> {
        self.read_store()?.has_blob_for_hash(hash)
    }

    pub fn get(&self, cid: &Cid) -> Result<Option<DBPinnableSlice<'_>>> {
        self.read_store()?.get(cid)
    }

    pub fn get_size(&self, cid: &Cid) -> Result<Option<usize>> {
        self.read_store()?.get_size(cid)
    }

    pub fn has(&self, cid: &Cid) -> Result<bool> {
        self.read_store()?.has(cid)
    }

    pub fn get_links(&self, cid: &Cid) -> Result<Option<Vec<Cid>>> {
        self.read_store()?.get_links(cid)
    }

    pub fn consistency_check(&self) -> Result<Vec<String>> {
        self.read_store()?.consistency_check()
    }

    fn write_store(&self) -> Result<WriteStore> {
        let db = &self.inner.content;
        Ok(WriteStore {
            db,
            cf: ColumnFamilies::new(db)?,
            next_id: self.inner.next_id.write().unwrap(),
        })
    }

    fn read_store(&self) -> Result<ReadStore> {
        let db = &self.inner.content;
        Ok(ReadStore {
            db,
            cf: ColumnFamilies::new(db)?,
            _next_id: self.inner.next_id.read().unwrap(),
        })
    }
}

/// Groups all write operations.
///
/// Not Send, so must be used from a single thread.
///
/// All write interacion with the database is done through this struct.
struct WriteStore<'a> {
    db: &'a RocksDb,
    cf: ColumnFamilies<'a>,
    next_id: RwLockWriteGuard<'a, u64>,
}

/// Groups all read operations.
///
/// Not Send, so must be used from a single thread.
///
/// All read interacion with the database is done through this struct.
struct ReadStore<'a> {
    db: &'a RocksDb,
    cf: ColumnFamilies<'a>,
    _next_id: RwLockReadGuard<'a, u64>,
}

struct ColumnFamilies<'a> {
    id: &'a ColumnFamily,
    metadata: &'a ColumnFamily,
    graph: &'a ColumnFamily,
    blobs: &'a ColumnFamily,
}

/// Struct used to iterate over all the ids for a multihash
struct CodeAndId {
    // the ipld code of the id
    #[allow(dead_code)]
    code: u64,
    // the id for the cid, used in most other column families
    id: u64,
}

impl<'a> ColumnFamilies<'a> {
    fn new(db: &'a RocksDb) -> anyhow::Result<Self> {
        Ok(Self {
            id: db
                .cf_handle(CF_ID_V0)
                .context("missing column family: id")?,
            metadata: db
                .cf_handle(CF_METADATA_V0)
                .context("missing column family: metadata")?,
            graph: db
                .cf_handle(CF_GRAPH_V0)
                .context("missing column family: graph")?,
            blobs: db
                .cf_handle(CF_BLOBS_V0)
                .context("missing column family: blobs")?,
        })
    }
}

impl<'a> WriteStore<'a> {
    fn put<T: AsRef<[u8]>, L>(&mut self, cid: Cid, blob: T, links: L) -> Result<()>
    where
        L: IntoIterator<Item = Cid>,
    {
        if self.has(&cid)? {
            return Ok(());
        }

        let id = self.next_id();

        let id_bytes = id.to_be_bytes();

        // guranteed that the key does not exists, so we want to store it

        let metadata = MetadataV0 {
            codec: cid.codec(),
            multihash: cid.hash().to_bytes(),
        };
        let metadata_bytes = rkyv::to_bytes::<_, 1024>(&metadata)?; // TODO: is this the right amount of scratch space?
        let id_key = id_key(&cid);

        let children = self.ensure_id_many(links.into_iter())?;

        let graph = GraphV0 { children };
        let graph_bytes = rkyv::to_bytes::<_, 1024>(&graph)?; // TODO: is this the right amount of scratch space?
        let mut batch = WriteBatch::default();
        batch.put_cf(self.cf.id, id_key, id_bytes);
        batch.put_cf(self.cf.blobs, id_bytes, blob);
        batch.put_cf(self.cf.metadata, id_bytes, metadata_bytes);
        batch.put_cf(self.cf.graph, id_bytes, graph_bytes);
        self.db.write(batch)?;

        Ok(())
    }

    fn put_many(&mut self, blocks: impl IntoIterator<Item = (Cid, Bytes, Vec<Cid>)>) -> Result<()> {
        let mut batch = WriteBatch::default();
        let mut cid_tracker: AHashSet<Cid> = AHashSet::default();
        for (cid, blob, links) in blocks.into_iter() {
            if cid_tracker.contains(&cid) || self.has(&cid)? {
                continue;
            }

            cid_tracker.insert(cid);

            let id = self.next_id();

            let id_bytes = id.to_be_bytes();

            // guranteed that the key does not exists, so we want to store it

            let metadata = MetadataV0 {
                codec: cid.codec(),
                multihash: cid.hash().to_bytes(),
            };
            let metadata_bytes = rkyv::to_bytes::<_, 1024>(&metadata)?; // TODO: is this the right amount of scratch space?
            let id_key = id_key(&cid);

            let children = self.ensure_id_many(links.into_iter())?;

            let graph = GraphV0 { children };
            let graph_bytes = rkyv::to_bytes::<_, 1024>(&graph)?; // TODO: is this the right amount of scratch space?

            batch.put_cf(self.cf.id, id_key, id_bytes);
            batch.put_cf(self.cf.blobs, id_bytes, blob);
            batch.put_cf(self.cf.metadata, id_bytes, metadata_bytes);
            batch.put_cf(self.cf.graph, id_bytes, graph_bytes);
        }

        self.db.write(batch)?;

        Ok(())
    }

    /// Takes a list of cids and gives them ids, which are both stored and then returned.
    fn ensure_id_many<I>(&mut self, cids: I) -> Result<Vec<u64>>
    where
        I: IntoIterator<Item = Cid>,
    {
        let mut ids = Vec::new();
        let mut batch = WriteBatch::default();
        for cid in cids {
            let id_key = id_key(&cid);
            let id = if let Some(id) = self.db.get_pinned_cf(self.cf.id, &id_key)? {
                u64::from_be_bytes(id.as_ref().try_into()?)
            } else {
                let id = self.next_id();
                let id_bytes = id.to_be_bytes();

                let metadata = MetadataV0 {
                    codec: cid.codec(),
                    multihash: cid.hash().to_bytes(),
                };
                let metadata_bytes = rkyv::to_bytes::<_, 1024>(&metadata)?; // TODO: is this the right amount of scratch space?
                batch.put_cf(&self.cf.id, id_key, id_bytes);
                batch.put_cf(&self.cf.metadata, id_bytes, metadata_bytes);
                id
            };
            ids.push(id);
        }
        self.db.write(batch)?;

        Ok(ids)
    }

    fn next_id(&mut self) -> u64 {
        let id = *self.next_id;
        if let Some(next_id) = self.next_id.checked_add(1) {
            *self.next_id = next_id;
        } else {
            panic!("this store is full");
        }
        id
    }

    fn get_id(&self, cid: &Cid) -> Result<Option<u64>> {
        let id_key = id_key(cid);
        let maybe_id_bytes = self.db.get_pinned_cf(self.cf.id, id_key)?;
        match maybe_id_bytes {
            Some(bytes) => {
                let arr = bytes[..8].try_into().map_err(|e| anyhow!("{:?}", e))?;
                Ok(Some(u64::from_be_bytes(arr)))
            }
            None => Ok(None),
        }
    }

    fn has(&self, cid: &Cid) -> Result<bool> {
        match self.get_id(cid)? {
            Some(id) => {
                let exists = self
                    .db
                    .get_pinned_cf(self.cf.blobs, id.to_be_bytes())?
                    .is_some();
                Ok(exists)
            }
            None => Ok(false),
        }
    }
}

impl<'a> ReadStore<'a> {
    fn get(&self, cid: &Cid) -> Result<Option<DBPinnableSlice<'a>>> {
        let res = match self.get_id(cid)? {
            Some(id) => {
                let maybe_blob = self.get_by_id(id)?;
                Ok(maybe_blob)
            }
            None => Ok(None),
        };
        res
    }

    fn get_size(&self, cid: &Cid) -> Result<Option<usize>> {
        match self.get_id(cid)? {
            Some(id) => {
                let maybe_size = self.get_size_by_id(id)?;
                Ok(maybe_size)
            }
            None => Ok(None),
        }
    }

    fn has(&self, cid: &Cid) -> Result<bool> {
        match self.get_id(cid)? {
            Some(id) => {
                let exists = self
                    .db
                    .get_pinned_cf(self.cf.blobs, id.to_be_bytes())?
                    .is_some();
                Ok(exists)
            }
            None => Ok(false),
        }
    }

    fn get_links(&self, cid: &Cid) -> Result<Option<Vec<Cid>>> {
        let res = match self.get_id(cid)? {
            Some(id) => {
                let maybe_links = self.get_links_by_id(id)?;
                Ok(maybe_links)
            }
            None => Ok(None),
        };
        res
    }

    fn get_id(&self, cid: &Cid) -> Result<Option<u64>> {
        let id_key = id_key(cid);
        let maybe_id_bytes = self.db.get_pinned_cf(self.cf.id, id_key)?;
        match maybe_id_bytes {
            Some(bytes) => {
                let arr = bytes[..8].try_into().map_err(|e| anyhow!("{:?}", e))?;
                Ok(Some(u64::from_be_bytes(arr)))
            }
            None => Ok(None),
        }
    }

    fn get_ids_for_hash(
        &self,
        hash: &Multihash,
    ) -> Result<impl Iterator<Item = Result<CodeAndId>> + 'a> {
        let hash = hash.to_bytes();
        let iter = self
            .db
            .iterator_cf(self.cf.id, IteratorMode::From(&hash, Direction::Forward));
        let hash_len = hash.len();
        Ok(iter
            .take_while(move |elem| {
                if let Ok((k, _)) = elem {
                    k.len() == hash_len + 8 && k.starts_with(&hash)
                } else {
                    // we don't want to swallow errors. An error is not the same as no result!
                    true
                }
            })
            .map(move |elem| {
                let (k, v) = elem?;
                let code = u64::from_be_bytes(k[hash_len..].try_into()?);
                let id = u64::from_be_bytes(v[..8].try_into()?);
                Ok(CodeAndId { code, id })
            }))
    }

    fn get_blob_by_hash(&self, hash: &Multihash) -> Result<Option<DBPinnableSlice<'a>>> {
        for elem in self.get_ids_for_hash(hash)? {
            let id = elem?.id;
            let id_bytes = id.to_be_bytes();
            if let Some(blob) = self.db.get_pinned_cf(self.cf.blobs, id_bytes)? {
                return Ok(Some(blob));
            }
        }
        Ok(None)
    }

    fn has_blob_for_hash(&self, hash: &Multihash) -> Result<bool> {
        for elem in self.get_ids_for_hash(hash)? {
            let id = elem?.id;
            let id_bytes = id.to_be_bytes();
            if let Some(_blob) = self.db.get_pinned_cf(self.cf.blobs, id_bytes)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn get_by_id(&self, id: u64) -> Result<Option<DBPinnableSlice<'a>>> {
        let maybe_blob = self.db.get_pinned_cf(self.cf.blobs, id.to_be_bytes())?;

        Ok(maybe_blob)
    }

    fn get_size_by_id(&self, id: u64) -> Result<Option<usize>> {
        let maybe_blob = self.db.get_pinned_cf(self.cf.blobs, id.to_be_bytes())?;
        let maybe_size = maybe_blob.map(|b| b.len());
        Ok(maybe_size)
    }

    fn get_links_by_id(&self, id: u64) -> Result<Option<Vec<Cid>>> {
        let id_bytes = id.to_be_bytes();
        // FIXME: can't use pinned because otherwise this can trigger alignment issues :/
        let cf = &self.cf;
        match self.db.get_cf(cf.graph, id_bytes)? {
            Some(links_id) => {
                let graph = rkyv::check_archived_root::<GraphV0>(&links_id)
                    .map_err(|e| anyhow!("{:?}", e))?;
                let keys = graph
                    .children
                    .iter()
                    .map(|id| (&cf.metadata, id.to_be_bytes()));
                let meta = self.db.multi_get_cf(keys);
                let mut links = Vec::with_capacity(meta.len());
                for (i, meta) in meta.into_iter().enumerate() {
                    match meta? {
                        Some(meta) => {
                            let meta = rkyv::check_archived_root::<MetadataV0>(&meta)
                                .map_err(|e| anyhow!("{:?}", e))?;
                            let multihash = cid::multihash::Multihash::from_bytes(&meta.multihash)?;
                            let c = cid::Cid::new_v1(meta.codec, multihash);
                            links.push(c);
                        }
                        None => {
                            bail!("invalid link: {}", graph.children[i]);
                        }
                    }
                }
                Ok(Some(links))
            }
            None => Ok(None),
        }
    }

    /// Perform an internal consistency check on the store, and return all internal errors found.
    fn consistency_check(&self) -> anyhow::Result<Vec<String>> {
        let mut res = Vec::new();
        let cf = &self.cf;
        let n_meta = self
            .db
            .iterator_cf(&cf.metadata, IteratorMode::Start)
            .count();
        let n_id = self.db.iterator_cf(&cf.id, IteratorMode::Start).count();
        if n_meta != n_id {
            res.push(format!(
                "non bijective mapping between cid and id. Metadata and id cfs have different lengths: {} != {}",
                n_meta, n_id
            ));
        }
        Ok(res)
    }
}

/// Column family to store actual data.
/// - Maps id (u64) to bytes
const CF_BLOBS_V0: &str = "blobs-v0";
/// Column family that stores metdata about a given blob.
/// - indexed by id (u64)
const CF_METADATA_V0: &str = "metadata-v0";
/// Column familty that stores the graph for a blob
/// - indexed by id (u64)
const CF_GRAPH_V0: &str = "graph-v0";
/// Column family that stores the mapping (multihash, code) to id.
///
/// By storing multihash first we can search for ids either by cid = (multihash, code) or by multihash.
const CF_ID_V0: &str = "id-v0";

// This wrapper type serializes the contained value out-of-line so that newer
// versions can be viewed as the older version.
#[derive(Debug, Archive, Deserialize, Serialize)]
#[repr(transparent)]
#[archive_attr(repr(transparent), derive(CheckBytes))]
struct Versioned<T>(#[with(AsBox)] T);

#[derive(Debug, Archive, Deserialize, Serialize)]
#[repr(C)]
#[archive_attr(repr(C), derive(CheckBytes))]
struct MetadataV0 {
    /// The codec of the original CID.
    codec: u64,
    multihash: Vec<u8>,
}

#[derive(Debug, Archive, Deserialize, Serialize)]
#[repr(C)]
#[archive_attr(repr(C), derive(CheckBytes))]
struct GraphV0 {
    children: Vec<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;

    use cid::multihash::{Code, MultihashDigest};
    const RAW: u64 = 0x55;

    #[tokio::test]
    async fn test_basics() {
        let dir = tempfile::tempdir().unwrap();

        let store = RockStore::create(dir.path().into()).await.unwrap();

        let mut values = Vec::new();

        for i in 0..100 {
            let data = vec![i as u8; i * 16];
            let hash = Code::Sha2_256.digest(&data);
            let c = cid::Cid::new_v1(RAW, hash);

            let link_hash = Code::Sha2_256.digest(&[(i + 1) as u8; 64]);
            let link = cid::Cid::new_v1(RAW, link_hash);

            let links = [link];

            store.put(c, &data, links).unwrap();
            values.push((c, data, links));
        }

        for (_i, (c, expected_data, expected_links)) in values.iter().enumerate() {
            assert!(store.has(c).unwrap());
            let data = store.get(c).unwrap().unwrap();
            assert_eq!(expected_data, &data[..]);

            let links = store.get_links(c).unwrap().unwrap();
            assert_eq!(expected_links, &links[..]);
        }
    }
}
