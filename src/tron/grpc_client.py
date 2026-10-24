import asyncio
from typing import Optional, Dict, Any, List
from concurrent.futures import ThreadPoolExecutor

try:
    import grpc
except ImportError:
    grpc = None

from ..config import (
    logger,
    CHAINSTACK_GRPC_HOST,
    CHAINSTACK_GRPC_PORT,
    CHAINSTACK_API_TOKEN,
    TRON_GRPC_ENDPOINTS,
    USE_GRPC,
)

_executor = ThreadPoolExecutor(max_workers=4)

# `except grpc.RpcError` raises AttributeError when grpcio is not installed
# (grpc is None), turning a missing dependency into a confusing crash inside the
# error handler. An empty tuple catches nothing and falls through to Exception.
_RPC_ERRORS = (grpc.RpcError,) if grpc else ()


def decode_varint(data: bytes, pos: int) -> tuple:
    """Decode a varint from bytes, return (value, new_position)."""
    result = 0
    shift = 0
    while pos < len(data):
        byte = data[pos]
        result |= (byte & 0x7F) << shift
        pos += 1
        if not (byte & 0x80):
            break
        shift += 7
    return result, pos


def parse_fields(data: bytes) -> List[tuple]:
    fields = []
    pos = 0
    while pos < len(data):
        tag = data[pos]
        field_num = tag >> 3
        wire_type = tag & 0x07
        pos += 1

        if wire_type == 0:
            value, pos = decode_varint(data, pos)
            fields.append((field_num, wire_type, value))
        elif wire_type == 2:
            length, pos = decode_varint(data, pos)
            value = data[pos : pos + length]
            pos += length
            fields.append((field_num, wire_type, value))
        elif wire_type == 1:
            pos += 8
        elif wire_type == 5:
            pos += 4
        else:
            break
    return fields


def parse_block_extension(data: bytes) -> Dict[str, Any]:
    """
    Parse BlockExtention protobuf (from GetNowBlock2).

    BlockExtention structure:
    - field 1 (repeated): TransactionExtention
    - field 2: BlockHeader
    - field 3: bytes (blockid)
    """
    result = {
        "block_number": 0,
        "timestamp": 0,
        "transactions": [],
        "_raw_size": len(data),
    }

    fields = parse_fields(data)

    for field_num, wire_type, value in fields:
        if field_num == 1 and wire_type == 2:
            tx = parse_transaction_ext(value)
            if tx:
                result["transactions"].append(tx)
        elif field_num == 2 and wire_type == 2:
            header = parse_block_header(value)
            result.update(header)

    return result


def parse_block_header(data: bytes) -> Dict[str, Any]:
    result = {}
    fields = parse_fields(data)

    for field_num, wire_type, value in fields:
        if field_num == 1 and wire_type == 2:
            # raw_data
            result.update(parse_raw_data(value))

    return result


def parse_raw_data(data: bytes) -> Dict[str, Any]:
    result = {}
    fields = parse_fields(data)

    for field_num, wire_type, value in fields:
        if wire_type == 0:
            if field_num == 1:
                result["timestamp"] = value
            elif field_num == 7:
                result["block_number"] = value

    return result


def parse_transaction_ext(data: bytes) -> Optional[Dict[str, Any]]:
    """
    Parse TransactionExtention.

    Structure:
    - field 1: Transaction
    - field 2: bytes txid
    """
    result = {
        "tx_id": "",
        "contract_address": "",
        "data": "",
    }

    fields = parse_fields(data)

    for field_num, wire_type, value in fields:
        if field_num == 1 and wire_type == 2:
            tx_data = parse_transaction_inner(value)
            result.update(tx_data)
        elif field_num == 2 and wire_type == 2:

            result["tx_id"] = value.hex()

    return result if result.get("tx_id") else None


def parse_transaction_inner(data: bytes) -> Dict[str, Any]:
    result = {}
    fields = parse_fields(data)

    for field_num, wire_type, value in fields:
        if field_num == 1 and wire_type == 2:
            result.update(parse_tx_raw_data(value))

    return result


def parse_tx_raw_data(data: bytes) -> Dict[str, Any]:
    result = {}
    fields = parse_fields(data)

    for field_num, wire_type, value in fields:
        if field_num == 11 and wire_type == 2:
            contract = parse_contract(value)
            result.update(contract)
            break

    return result


def parse_contract(data: bytes) -> Dict[str, Any]:
    result = {}
    fields = parse_fields(data)

    for field_num, wire_type, value in fields:
        if field_num == 1 and wire_type == 0:
            result["contract_type"] = value
        elif field_num == 2 and wire_type == 2:
            param = parse_any_parameter(value)
            result.update(param)

    return result


def parse_any_parameter(data: bytes) -> Dict[str, Any]:
    result = {}
    fields = parse_fields(data)

    for field_num, wire_type, value in fields:
        if field_num == 2 and wire_type == 2:
            result.update(parse_trigger_smart_contract(value))

    return result


def parse_trigger_smart_contract(data: bytes) -> Dict[str, Any]:
    result = {}
    fields = parse_fields(data)

    for field_num, wire_type, value in fields:
        if wire_type == 2:
            if field_num == 1:
                result["owner_address"] = value.hex()
            elif field_num == 2:
                result["contract_address"] = value.hex()
            elif field_num == 4:
                result["data"] = value.hex()

    return result


def grpc_available() -> bool:
    # The resolved endpoint list is authoritative: TRON_NODES may deliberately
    # list only HTTP nodes, and falling back to the raw CHAINSTACK_API_TOKEN
    # would then dial a gRPC endpoint the operator did not ask for. The list is
    # built from those same tokens when TRON_NODES is unset, so a legacy
    # deployment is unaffected.
    return bool(grpc) and USE_GRPC and bool(TRON_GRPC_ENDPOINTS)


class TronGrpcClient:

    def __init__(
        self,
        host: str = None,
        port: int = None,
        api_token: str = None,
    ):
        if host or api_token:
            # Explicit single endpoint (tests, one-off probes): no failover.
            self._endpoints = [(
                host or CHAINSTACK_GRPC_HOST,
                port or CHAINSTACK_GRPC_PORT,
                api_token or CHAINSTACK_API_TOKEN,
            )]
        else:
            # No synthesised fallback: an empty list means no gRPC endpoint is
            # configured, and inventing one from the raw CHAINSTACK_* values
            # would ignore an explicit HTTP-only TRON_NODES.
            self._endpoints = list(TRON_GRPC_ENDPOINTS)
        self._ep_index = 0
        # An empty list is legitimate now (HTTP-only TRON_NODES). Keep the
        # attributes defined so callers and logs do not need a special case;
        # grpc_available() already stops the watchers reaching this client.
        self.host, self.port, self.api_token = (
            self._endpoints[0] if self._endpoints else ("", 0, "")
        )
        self._channel = None
        self._initialized = False
        self.failovers = 0          # surfaced for /health

    def _init_channel(self):
        """Initialize gRPC channel with authentication."""
        if not grpc:
            raise RuntimeError("grpcio is not installed")
        if self._initialized and self._channel:
            return

        try:

            def add_auth_header(callback, *args, **kwargs):
                callback([("x-token", self.api_token)], None)

            call_credentials = grpc.metadata_call_credentials(
                lambda context, callback: add_auth_header(callback)
            )

            channel_credentials = grpc.ssl_channel_credentials()
            composite_credentials = grpc.composite_channel_credentials(
                channel_credentials, call_credentials
            )

            self._channel = grpc.secure_channel(
                f"{self.host}:{self.port}",
                composite_credentials,
                options=[
                    ("grpc.max_receive_message_length", 50 * 1024 * 1024),
                    ("grpc.keepalive_time_ms", 30000),
                    ("grpc.keepalive_timeout_ms", 10000),
                ],
            )

            self._initialized = True
            logger.info(f"gRPC connected to {self.host}:{self.port}")

        except Exception as e:
            logger.error(f"gRPC init failed: {e}")
            raise

    def _switch_endpoint(self) -> bool:
        """Advance to the next endpoint. False when there is nowhere to go."""
        if len(self._endpoints) < 2:
            return False
        self._ep_index = (self._ep_index + 1) % len(self._endpoints)
        self.host, self.port, self.api_token = self._endpoints[self._ep_index]
        if self._channel:
            try:
                self._channel.close()
            except Exception:
                pass
        self._channel = None
        self._initialized = False
        self.failovers += 1
        logger.warning(
            f"TRON gRPC failing over to endpoint {self._ep_index + 1}"
            f"/{len(self._endpoints)}: {self.host}:{self.port}"
        )
        return True

    def _invoke(self, path, payload, parser, timeout=5, empty_is_failure=False):
        """Call one unary method, failing over across endpoints.

        empty_is_failure: set for calls whose result must exist (the chain head).
        A dead token answers 200-at-the-transport but yields nothing useful, so
        "no data" has to count as a failure or we never leave a broken endpoint.
        Left False for lookups where None legitimately means "not found".
        """
        attempts = max(1, len(self._endpoints))
        for _ in range(attempts):
            reason = None
            try:
                self._init_channel()
                method = self._channel.unary_unary(
                    path,
                    request_serializer=lambda x: x,
                    response_deserializer=lambda x: x,
                )
                result = parser(method(payload, timeout=timeout))
                if result is not None or not empty_is_failure:
                    return result
                reason = "returned no data"
            except _RPC_ERRORS as e:
                reason = f"{e.code()} - {e.details()}"
            except Exception as e:
                reason = f"{type(e).__name__}: {e}"

            logger.error(f"gRPC {path} via {self.host}:{self.port} -> {reason}")
            if not self._switch_endpoint():
                break
        return None

    def close(self):
        if self._channel:
            self._channel.close()
            self._initialized = False

    def _encode_varint(self, value: int) -> bytes:
        result = bytearray()
        while value > 0x7F:
            result.append((value & 0x7F) | 0x80)
            value >>= 7
        result.append(value & 0x7F)
        return bytes(result) if result else b"\x00"

    def _encode_number_message(self, num: int) -> bytes:
        result = bytearray()
        result.append(0x08)
        result.extend(self._encode_varint(num))
        return bytes(result)

    def _encode_bytes_message(self, data: bytes) -> bytes:
        result = bytearray()
        result.append(0x0A)
        result.extend(self._encode_varint(len(data)))
        result.extend(data)
        return bytes(result)

    def get_now_block(self) -> Optional[Dict[str, Any]]:
        # The chain head always exists: None here means this endpoint is broken
        # (expired token answers PERMISSION_DENIED), so fail over.
        return self._invoke(
            "/protocol.Wallet/GetNowBlock2", b"", parse_block_extension,
            empty_is_failure=True,
        )

    def get_block_by_num(self, block_num: int) -> Optional[Dict[str, Any]]:
        """Get block by number."""
        # A block we asked for by number should exist, so empty = unhealthy.
        return self._invoke(
            "/protocol.Wallet/GetBlockByNum2",
            self._encode_number_message(block_num),
            parse_block_extension,
            empty_is_failure=True,
        )

    def get_transaction_by_id(self, tx_hash: str) -> Optional[Dict[str, Any]]:
        """Get transaction by hash."""
        # None can legitimately mean "no such tx", so do NOT fail over on empty;
        # only transport errors rotate the endpoint.
        return self._invoke(
            "/protocol.Wallet/GetTransactionById",
            self._encode_bytes_message(bytes.fromhex(tx_hash)),
            lambda r: parse_transaction_inner(r) if r else None,
        )


_grpc_client: Optional[TronGrpcClient] = None


def active_endpoint_index() -> int:
    """Index into TRON_GRPC_ENDPOINTS that detection is currently using."""
    return _grpc_client._ep_index if _grpc_client else 0


def get_grpc_client() -> TronGrpcClient:
    global _grpc_client
    if _grpc_client is None:
        _grpc_client = TronGrpcClient()
    return _grpc_client


async def grpc_get_now_block() -> Optional[Dict[str, Any]]:
    if not grpc_available():
        return None
    client = get_grpc_client()
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, client.get_now_block)


async def grpc_get_block_by_num(block_num: int) -> Optional[Dict[str, Any]]:
    if not grpc_available():
        return None
    client = get_grpc_client()
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, client.get_block_by_num, block_num)


async def grpc_get_transaction_by_id(tx_hash: str) -> Optional[Dict[str, Any]]:
    if not grpc_available():
        return None
    client = get_grpc_client()
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(_executor, client.get_transaction_by_id, tx_hash)
