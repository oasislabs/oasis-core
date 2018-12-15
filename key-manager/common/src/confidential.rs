//! Encryption utilties for Web3(c).
//! Wraps the ekiden_core::mrae::sivaessha2 primitives with a set of encryption
//! methods that transparently encodes/decodes the Web3(c) wire format.

use ekiden_core::error::{Error, Result};
use ekiden_core::mrae::sivaessha2;

use super::{PrivateKeyType, PublicKeyType, StateKeyType, EMPTY_PRIVATE_KEY, EMPTY_PUBLIC_KEY,
            EMPTY_STATE_KEY};

/// Encrypts the given plaintext using the symmetric key derived from
/// peer_public_key and secret_key. Uses the given public_key to return
/// an encrypted payload of the form: nonce || public_key || cipher,
/// Allowing the receipient of the encrypted payload to decrypt with
/// the given nonce and public_key.
pub fn encrypt(
    plaintext: Vec<u8>,
    nonce: Vec<u8>,
    peer_public_key: PublicKeyType,
    public_key: &PublicKeyType,
    secret_key: &PrivateKeyType,
) -> Result<Vec<u8>> {
    let ciphertext = sivaessha2::box_seal(
        nonce.clone(),
        plaintext.clone(),
        vec![],
        peer_public_key.into(),
        *secret_key,
    )?;
    Ok(encode_encryption(ciphertext, nonce, *public_key))
}

/// Decrypts the given payload generated in the same manner by the encrypt method.
/// I.e., given an encrypted payload of the form nonce || public_key || cipher,
/// extracts the nonce and public key and uses them along with the given secret_key
/// the decrypt the cipher, returning the resulting Decryption struct.
pub fn decrypt(data: Option<Vec<u8>>, secret_key: &PrivateKeyType) -> Result<Decryption> {
    if data.is_none() {
        return Ok(Decryption {
            plaintext: Default::default(),
            peer_public_key: Default::default(),
            nonce: Default::default(),
        });
    }
    let (nonce, peer_public_key, cipher) = split_encrypted_payload(data.unwrap())?;
    let plaintext = sivaessha2::box_open(
        nonce.clone(),
        cipher,
        vec![],
        peer_public_key.into(),
        *secret_key,
    )?;
    Ok(Decryption {
        plaintext,
        peer_public_key,
        nonce: nonce,
    })
}

/// The returned result of decrypting an encrypted payload, where
/// nonce and peer_public_key were used to encrypt the plaintext.
#[derive(Debug, Clone)]
pub struct Decryption {
    pub plaintext: Vec<u8>,
    pub nonce: Vec<u8>,
    pub peer_public_key: PublicKeyType,
}

/// Packs the given paramaters into a Vec of the form nonce || public_key || ciphertext.
fn encode_encryption(
    mut ciphertext: Vec<u8>,
    nonce: Vec<u8>,
    public_key: PublicKeyType,
) -> Vec<u8> {
    let mut encryption = nonce;
    encryption.append(&mut public_key.to_vec());
    encryption.append(&mut ciphertext);
    encryption
}

/// Assumes data is of the form  IV || PK || CIPHER.
/// Returns a tuple of each component.
fn split_encrypted_payload(data: Vec<u8>) -> Result<(Vec<u8>, PublicKeyType, Vec<u8>)> {
    let nonce_size = sivaessha2::NONCE_SIZE;
    if data.len() < nonce_size + 32 {
        return Err(Error::new("Invalid nonce or public key"));
    }
    let nonce = data[..nonce_size].to_vec();
    let mut peer_public_key = EMPTY_PUBLIC_KEY;
    peer_public_key.copy_from_slice(&data[nonce_size..nonce_size + 32]);
    let cipher = data[nonce_size + 32..].to_vec();
    Ok((nonce, peer_public_key, cipher))
}

/// Hard coded key manager retrieved contract keys for Web3(c) V0.5.
/// Public key = 0x9385b8391e06d67c3de1675a58cffc3ad16bcf7cc56ab35d7db1fc03fb227a54.
/// Private key = 0xd5af0c986e6a9cce52d05803e962d4b19f915905debcb41f35b68eebc954fa49.
pub fn default_contract_keys() -> (PublicKeyType, PrivateKeyType, StateKeyType) {
    let seed = [
        213, 175, 12, 152, 110, 106, 156, 206, 82, 208, 88, 3, 233, 98, 212, 177, 159, 145, 89, 5,
        222, 188, 180, 31, 53, 182, 142, 235, 201, 84, 250, 73,
    ];
    let mut public_key = EMPTY_PUBLIC_KEY;
    let mut private_key = EMPTY_PRIVATE_KEY;
    let mut state_key = EMPTY_STATE_KEY;
    sodalite::box_keypair_seed(&mut public_key, &mut private_key, &seed);

    (public_key, private_key, state_key)
}
