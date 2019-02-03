//! Signature interface.
use std::{self, convert::TryFrom, marker::PhantomData, sync::Arc};

use serde::{de::DeserializeOwned, Serialize};
use serde_bytes;
use serde_cbor;

use super::{
    bytes::{B256, B512, B64, H256},
    error::{Error, Result},
    ring::{
        digest,
        signature::{self, KeyPair},
    },
    untrusted,
};

use ekiden_common_api as api;

/// Signer interface.
pub trait Signer: Sync + Send {
    /// Sign given 256-bit digest.
    fn sign(&self, data: &H256) -> B512;

    /// Get the signing public key.
    fn get_public_key(&self) -> B256;

    /// Attest to given 256-bit digest.
    fn attest(&self, data: &H256) -> Option<Vec<u8>>;
}

impl<T: ?Sized + Signer> Signer for Arc<T> {
    fn sign(&self, data: &H256) -> B512 {
        Signer::sign(&**self, data)
    }

    fn get_public_key(&self) -> B256 {
        Signer::get_public_key(&**self)
    }

    fn attest(&self, data: &H256) -> Option<Vec<u8>> {
        Signer::attest(&**self, data)
    }
}

/// Verifier interface.
pub trait Verifier {
    /// Verify signature and optional attestation.
    fn verify(&self, data: &H256, signature: &B512, attestation: Option<&Vec<u8>>) -> bool;
}

impl<T: ?Sized + Verifier> Verifier for Arc<T> {
    fn verify(&self, data: &H256, signature: &B512, attestation: Option<&Vec<u8>>) -> bool {
        Verifier::verify(&**self, data, signature, attestation)
    }
}

/// Null signer/verifier which does no signing and says everything is verified.
///
/// **This should only be used in tests.**
pub struct NullSignerVerifier;

impl Signer for NullSignerVerifier {
    fn sign(&self, _data: &H256) -> B512 {
        B512::zero()
    }

    fn get_public_key(&self) -> B256 {
        B256::zero()
    }

    fn attest(&self, _data: &H256) -> Option<Vec<u8>> {
        None
    }
}

impl Verifier for NullSignerVerifier {
    fn verify(&self, _data: &H256, _signature: &B512, _attestation: Option<&Vec<u8>>) -> bool {
        true
    }
}

/// In memory signer.
pub struct InMemorySigner {
    /// Ed25519 key pair.
    key_pair: signature::Ed25519KeyPair,
}

impl InMemorySigner {
    /// Construct new in memory key pair.
    pub fn new(key_pair: signature::Ed25519KeyPair) -> Self {
        Self { key_pair }
    }
}

impl Signer for InMemorySigner {
    fn sign(&self, data: &H256) -> B512 {
        B512::from(self.key_pair.sign(data).as_ref())
    }

    fn get_public_key(&self) -> B256 {
        B256::from(self.key_pair.public_key().as_ref())
    }

    fn attest(&self, _data: &H256) -> Option<Vec<u8>> {
        None
    }
}

/// Public key verifier.
pub struct PublicKeyVerifier<'a> {
    /// Public key.
    public_key: &'a B256,
}

impl<'a> PublicKeyVerifier<'a> {
    pub fn new(public_key: &'a B256) -> Self {
        Self { public_key }
    }
}

impl<'a> Verifier for PublicKeyVerifier<'a> {
    fn verify(&self, data: &H256, signature: &B512, attestation: Option<&Vec<u8>>) -> bool {
        // TODO: Verify attestation.
        match attestation {
            Some(_) => return false,
            None => {}
        }

        signature::verify(
            &signature::ED25519,
            untrusted::Input::from(self.public_key),
            untrusted::Input::from(&data),
            untrusted::Input::from(&signature),
        )
        .is_ok()
    }
}

/// Signature from a committee node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signature {
    /// Public key that made the signature.
    pub public_key: B256,
    /// Ed25519 signature.
    pub signature: B512,
    /// Optional attestation verification report in case the runtime is being executed
    /// in a TEE, attesting to the fact that a trusted hardware platform running specific
    /// code generated the signature.
    pub attestation: Option<Vec<u8>>,
}

impl Signature {
    /// Compute SHA-512/256 digest over (context, value).
    fn digest(context: &B64, value: &[u8]) -> H256 {
        let mut ctx = digest::Context::new(&digest::SHA512_256);
        ctx.update(context);
        ctx.update(value);
        H256::from(ctx.finish().as_ref())
    }

    /// Sign given value in given context using the given signer.
    pub fn sign(signer: &Signer, context: &B64, value: &[u8]) -> Self {
        let digest = Self::digest(context, value);

        Signature {
            public_key: signer.get_public_key(),
            signature: signer.sign(&digest),
            attestation: signer.attest(&digest),
        }
    }

    /// Verify signature and optional attestation.
    ///
    /// Note that you need to ensure that the attestation is actually present if
    /// attestation is required.
    pub fn verify(&self, context: &B64, value: &[u8]) -> bool {
        let digest = Self::digest(context, value);
        let verifier = PublicKeyVerifier::new(&self.public_key);

        verifier.verify(&digest, &self.signature, self.attestation.as_ref())
    }
}

impl TryFrom<api::Signature> for Signature {
    type Error = super::error::Error;
    //TODO: attestation.
    fn try_from(a: api::Signature) -> std::result::Result<Self, self::Error> {
        let pk = a.get_pubkey();
        let sig = a.get_signature();
        if pk.len() != 32 || sig.len() != 64 {
            return Err(Error::new("corrupted signature"));
        }

        let mut out = Signature {
            public_key: B256::zero(),
            signature: B512::zero(),
            attestation: None,
        };
        out.public_key.copy_from_slice(&pk);
        out.signature.copy_from_slice(&sig);
        Ok(out)
    }
}

impl Into<api::Signature> for Signature {
    // TODO: attestation.
    fn into(self) -> api::Signature {
        let mut s = api::Signature::new();
        s.set_pubkey(self.public_key.to_vec());
        s.set_signature(self.signature.to_vec());
        s
    }
}

/// Signature from a committee node.
#[derive(Debug, Serialize, Deserialize)]
pub struct Signed<T> {
    /// Untrusted serialized value.
    #[serde(with = "serde_bytes")]
    untrusted_raw_value: Vec<u8>,
    /// Act as if we own a T.
    #[serde(skip)]
    value: PhantomData<T>,
    /// Signature.
    pub signature: Signature,
}

impl<T> Signed<T> {
    /// Sign a new value.
    pub fn sign(signer: &Signer, context: &B64, value: T) -> Self
    where
        T: Serialize,
    {
        let untrusted_raw_value = serde_cbor::to_vec(&value).unwrap();
        let signature = Signature::sign(signer, context, &untrusted_raw_value);

        Self {
            untrusted_raw_value,
            value: PhantomData,
            signature,
        }
    }

    /// Verify signature and return signed value.
    pub fn open(&self, context: &B64) -> Result<T>
    where
        T: DeserializeOwned,
    {
        // First verify signature.
        if !self.signature.verify(context, &self.untrusted_raw_value) {
            return Err(Error::new("signature verification failed"));
        }

        self.get_value_unsafe()
    }

    /// Return value without verifying signature.
    ///
    /// Only use this variant if you have verified the signature yourself.
    pub fn get_value_unsafe(&self) -> Result<T>
    where
        T: DeserializeOwned,
    {
        Ok(serde_cbor::from_slice(&self.untrusted_raw_value)?)
    }

    /// Create a signed object from a detached signature.
    pub fn from_parts(value: T, signature: Signature) -> Self
    where
        T: Serialize,
    {
        Self {
            untrusted_raw_value: serde_cbor::to_vec(&value).unwrap(),
            value: PhantomData,
            signature,
        }
    }
}

impl<T: Clone> Clone for Signed<T> {
    fn clone(&self) -> Self {
        Signed {
            untrusted_raw_value: self.untrusted_raw_value.clone(),
            value: PhantomData,
            signature: self.signature.clone(),
        }
    }
}

impl<T> TryFrom<api::Signed> for Signed<T> {
    type Error = super::error::Error;

    fn try_from(mut pb: api::Signed) -> std::result::Result<Self, self::Error> {
        Ok(Signed {
            untrusted_raw_value: pb.take_blob(),
            value: PhantomData::<T>,
            signature: Signature::try_from(pb.take_signature())?,
        })
    }
}

impl<T> Into<api::Signed> for Signed<T> {
    fn into(self) -> api::Signed {
        let mut signed = api::Signed::new();
        signed.set_blob(self.untrusted_raw_value);
        signed.set_signature(self.signature.into());
        signed
    }
}
