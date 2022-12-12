use anyhow::Result;
use libipld::cid::multihash::{Code, Multihash, MultihashDigest};
use libipld::cid::{Cid, Version};
use unsigned_varint::{decode as varint_decode, encode as varint_encode};

#[derive(PartialEq, Eq, Clone, Debug, Copy)]
pub struct Prefix {
    pub version: Version,
    pub codec: u64,
    pub mh_type: u64,
    pub mh_len: usize,
}

impl Prefix {
    // default to cid v1 with default hash length
    pub fn new(codec: u64, mh_type: u64) -> Self {
        Prefix {
            version: Version::V1,
            codec,
            mh_type,
            mh_len: 0,
        }
    }

    pub fn new_from_bytes(data: &[u8]) -> Result<Prefix> {
        let (raw_version, remain) = varint_decode::u64(data)?;
        let version = Version::try_from(raw_version)?;
        let (codec, remain) = varint_decode::u64(remain)?;
        let (mh_type, remain) = varint_decode::u64(remain)?;
        let (mh_len, _remain) = varint_decode::usize(remain)?;

        Ok(Prefix {
            version,
            codec,
            mh_type,
            mh_len,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut res = Vec::with_capacity(4);

        let mut buf = varint_encode::u64_buffer();
        let version = varint_encode::u64(self.version.into(), &mut buf);
        res.extend_from_slice(version);
        let mut buf = varint_encode::u64_buffer();
        let codec = varint_encode::u64(self.codec, &mut buf);
        res.extend_from_slice(codec);
        let mut buf = varint_encode::u64_buffer();
        let mh_type = varint_encode::u64(self.mh_type.into(), &mut buf);
        res.extend_from_slice(mh_type);
        let mut buf = varint_encode::u64_buffer();
        let mh_len = varint_encode::u64(self.mh_len as u64, &mut buf);
        res.extend_from_slice(mh_len);

        res
    }

    pub fn to_cid(&self, data: &[u8]) -> Result<Cid> {
        let hash: Multihash = Code::try_from(self.mh_type)?.digest(data);
        let cid = Cid::new(self.version, self.codec, hash)?;
        Ok(cid)
    }
}

impl From<&Cid> for Prefix {
    fn from(cid: &Cid) -> Self {
        Self {
            version: cid.version(),
            codec: cid.codec(),
            mh_type: cid.hash().code(),
            mh_len: cid.hash().size().into(),
        }
    }
}
