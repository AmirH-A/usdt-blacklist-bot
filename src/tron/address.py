import hashlib
from typing import Optional

BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


def tron_address_to_hex(address: str, with_prefix: bool = False) -> Optional[str]:
    if not address or not address.startswith("T"):
        return None
    try:
        num = 0
        for char in address:
            num = num * 58 + BASE58_ALPHABET.index(char)
        hex_bytes = hex(num)[2:].zfill(50)
        return hex_bytes[:42] if with_prefix else hex_bytes[2:42]
    except Exception:
        return None


def hex_to_tron_address(hex_address: str) -> Optional[str]:
    try:
        if not hex_address.startswith("41"):
            hex_address = "41" + hex_address
        address_bytes = bytes.fromhex(hex_address)
        hash1 = hashlib.sha256(address_bytes).digest()
        hash2 = hashlib.sha256(hash1).digest()
        checksum = hash2[:4]
        full_address = address_bytes + checksum
        num = int.from_bytes(full_address, "big")
        result = ""
        while num > 0:
            num, remainder = divmod(num, 58)
            result = BASE58_ALPHABET[remainder] + result
        for byte in full_address:
            if byte == 0:
                result = "1" + result
            else:
                break
        return result
    except Exception:
        return None


def is_valid_tron_address(address: str) -> bool:
    if not address or len(address) != 34 or not address.startswith("T"):
        return False
    return all(c in BASE58_ALPHABET for c in address)
