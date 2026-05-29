#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Peer P2P elementar para transferência de arquivos por blocos.

Cada processo atua simultaneamente como servidor TCP (atendendo requisições
META, HAVE e GET) e como cliente (solicitando blocos aos vizinhos estáticos).
A implementação foi mantida sem dependências externas para facilitar a execução
em laboratórios acadêmicos.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import logging
import os
import random
import shutil
import signal
import socket
import socketserver
import sys
import threading
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

ENCODING = "utf-8"
DEFAULT_TIMEOUT = 3.0
PROTOCOL_VERSION = "sd-p2p-v1"


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def parse_neighbor(value: str) -> Tuple[str, int]:
    if ":" not in value:
        raise ValueError(f"Vizinho inválido: {value!r}. Use host:porta.")
    host, port = value.rsplit(":", 1)
    return host.strip(), int(port.strip())


def parse_neighbors(value: str) -> List[Tuple[str, int]]:
    if not value:
        return []
    return [parse_neighbor(item) for item in value.split(",") if item.strip()]


@dataclass
class FileMetadata:
    file_name: str
    file_size: int
    block_size: int
    total_blocks: int
    file_sha256: str
    block_sha256: List[str]

    @classmethod
    def from_file(cls, path: Path, block_size: int) -> "FileMetadata":
        block_hashes: List[str] = []
        file_size = path.stat().st_size
        with path.open("rb") as stream:
            while True:
                chunk = stream.read(block_size)
                if not chunk:
                    break
                block_hashes.append(sha256_bytes(chunk))
        return cls(
            file_name=path.name,
            file_size=file_size,
            block_size=block_size,
            total_blocks=len(block_hashes),
            file_sha256=sha256_file(path),
            block_sha256=block_hashes,
        )

    @classmethod
    def from_dict(cls, data: Dict) -> "FileMetadata":
        return cls(
            file_name=str(data["file_name"]),
            file_size=int(data["file_size"]),
            block_size=int(data["block_size"]),
            total_blocks=int(data["total_blocks"]),
            file_sha256=str(data["file_sha256"]),
            block_sha256=list(data["block_sha256"]),
        )

    def to_dict(self) -> Dict:
        return asdict(self)


def save_metadata(metadata: FileMetadata, path: Path) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(metadata.to_dict(), indent=2, ensure_ascii=False), encoding=ENCODING)


def load_metadata(path: Path) -> FileMetadata:
    return FileMetadata.from_dict(json.loads(path.read_text(encoding=ENCODING)))


class JsonLineSocket:
    """Pequeno empacotador de mensagens JSON terminadas por '\n'."""

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.file = sock.makefile("rwb")

    def send_json(self, message: Dict) -> None:
        payload = json.dumps(message, ensure_ascii=False, separators=(",", ":")).encode(ENCODING) + b"\n"
        self.file.write(payload)
        self.file.flush()

    def read_json(self) -> Dict:
        line = self.file.readline()
        if not line:
            raise ConnectionError("conexão encerrada antes de receber JSON")
        return json.loads(line.decode(ENCODING))

    def send_bytes(self, data: bytes) -> None:
        self.file.write(data)
        self.file.flush()

    def read_exact(self, length: int) -> bytes:
        chunks = []
        remaining = length
        while remaining > 0:
            chunk = self.file.read(remaining)
            if not chunk:
                raise ConnectionError("conexão encerrada durante leitura binária")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def close(self) -> None:
        try:
            self.file.close()
        finally:
            self.sock.close()


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


class PeerRequestHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        peer: "PeerNode" = self.server.peer_node  # type: ignore[attr-defined]
        self.request.settimeout(peer.socket_timeout)
        channel = JsonLineSocket(self.request)
        try:
            request = channel.read_json()
            response_type = request.get("type")
            if response_type == "HELLO":
                channel.send_json({
                    "type": "HELLO_OK",
                    "version": PROTOCOL_VERSION,
                    "peer_id": peer.peer_id,
                    "known_files": peer.known_file_names(),
                })
            elif response_type == "META":
                file_name = request.get("file_name")
                metadata = peer.get_metadata(str(file_name)) if file_name else peer.first_metadata()
                if metadata:
                    channel.send_json({"type": "META_OK", "metadata": metadata.to_dict(), "peer_id": peer.peer_id})
                    peer.log.info("META enviado para %s", self.client_address)
                else:
                    channel.send_json({"type": "ERROR", "message": "metadado não encontrado", "peer_id": peer.peer_id})
            elif response_type == "HAVE":
                file_name = str(request.get("file_name"))
                bitfield = peer.have_bitfield(file_name)
                channel.send_json({"type": "HAVE_OK", "file_name": file_name, "bitfield": bitfield, "peer_id": peer.peer_id})
            elif response_type == "GET_MANY":
                file_name = str(request.get("file_name"))
                indices = [int(idx) for idx in request.get("indices", [])]
                items = []
                payloads = []
                for index in indices:
                    block = peer.read_block(file_name, index)
                    if block is None:
                        continue
                    items.append({"index": index, "length": len(block), "sha256": sha256_bytes(block)})
                    payloads.append(block)
                if not items:
                    channel.send_json({"type": "ERROR", "message": "nenhum bloco disponível", "peer_id": peer.peer_id})
                    peer.log.info("GET_MANY nenhum bloco arquivo=%s para=%s", file_name, self.client_address)
                else:
                    if peer.artificial_delay > 0:
                        time.sleep(peer.artificial_delay * len(items))
                    channel.send_json({
                        "type": "BLOCKS",
                        "file_name": file_name,
                        "count": len(items),
                        "items": items,
                        "peer_id": peer.peer_id,
                    })
                    for block in payloads:
                        channel.send_bytes(block)
                    peer.log.info("SERVED_MANY blocos=%s bytes=%s arquivo=%s para=%s", len(items), sum(item["length"] for item in items), file_name, self.client_address)
            elif response_type == "GET":
                file_name = str(request.get("file_name"))
                index = int(request.get("index"))
                block = peer.read_block(file_name, index)
                if block is None:
                    channel.send_json({"type": "ERROR", "message": "bloco não disponível", "peer_id": peer.peer_id})
                    peer.log.info("GET bloco=%s arquivo=%s indisponível para %s", index, file_name, self.client_address)
                else:
                    if peer.artificial_delay > 0:
                        time.sleep(peer.artificial_delay)
                    digest = sha256_bytes(block)
                    channel.send_json({
                        "type": "BLOCK",
                        "file_name": file_name,
                        "index": index,
                        "length": len(block),
                        "sha256": digest,
                        "peer_id": peer.peer_id,
                    })
                    channel.send_bytes(block)
                    peer.log.info("SERVED bloco=%s bytes=%s arquivo=%s para=%s", index, len(block), file_name, self.client_address)
            else:
                channel.send_json({"type": "ERROR", "message": f"tipo de mensagem desconhecido: {response_type!r}"})
        except Exception as exc:  # pragma: no cover - log defensivo
            peer.log.warning("falha atendendo %s: %s", self.client_address, exc)
        finally:
            try:
                channel.close()
            except Exception:
                pass


class PeerNode:
    def __init__(
        self,
        peer_id: str,
        host: str,
        port: int,
        neighbors: Sequence[Tuple[str, int]],
        data_dir: Path,
        source_file: Optional[Path] = None,
        target_file_name: Optional[str] = None,
        metadata_path: Optional[Path] = None,
        block_size: int = 1024,
        socket_timeout: float = DEFAULT_TIMEOUT,
        request_interval: float = 0.05,
        max_runtime: float = 60.0,
        exit_when_complete: bool = False,
        serve_only: bool = False,
        artificial_delay: float = 0.0,
    ) -> None:
        self.peer_id = peer_id
        self.host = host
        self.port = port
        self.neighbors = list(neighbors)
        self.data_dir = data_dir
        self.shared_dir = data_dir / "shared"
        self.blocks_root = data_dir / "blocks"
        self.download_dir = data_dir / "downloads"
        self.meta_dir = data_dir / "metadata"
        self.logs_dir = data_dir / "logs"
        self.block_size = block_size
        self.socket_timeout = socket_timeout
        self.request_interval = request_interval
        self.max_runtime = max_runtime
        self.exit_when_complete = exit_when_complete
        self.serve_only = serve_only
        self.artificial_delay = artificial_delay
        self.stop_event = threading.Event()
        self.metadata_by_file: Dict[str, FileMetadata] = {}
        self.verified_blocks: Dict[str, set[int]] = {}
        self.state_lock = threading.RLock()
        self.server: Optional[ThreadedTCPServer] = None
        self.target_file_name = target_file_name
        self.source_file = source_file

        for directory in (self.shared_dir, self.blocks_root, self.download_dir, self.meta_dir, self.logs_dir):
            ensure_dir(directory)
        self.log = self._setup_logger()

        if source_file:
            source_file = source_file.resolve()
            metadata = FileMetadata.from_file(source_file, block_size)
            self.metadata_by_file[metadata.file_name] = metadata
            self.target_file_name = self.target_file_name or metadata.file_name
            save_metadata(metadata, self.meta_dir / f"{metadata.file_name}.meta.json")
            # Mantém uma cópia no diretório compartilhado do peer, para leitura por fatias.
            target_shared = self.shared_dir / metadata.file_name
            if source_file != target_shared.resolve():
                shutil.copyfile(source_file, target_shared)
            self.log.info("Seeder inicial: arquivo=%s tamanho=%s blocos=%s sha256=%s", metadata.file_name, metadata.file_size, metadata.total_blocks, metadata.file_sha256)

        if metadata_path:
            metadata = load_metadata(metadata_path)
            self.metadata_by_file[metadata.file_name] = metadata
            self.target_file_name = self.target_file_name or metadata.file_name
            save_metadata(metadata, self.meta_dir / f"{metadata.file_name}.meta.json")
            self.log.info("Metadado carregado: arquivo=%s tamanho=%s blocos=%s", metadata.file_name, metadata.file_size, metadata.total_blocks)

        # Recarrega metadados persistidos, se existirem.
        for meta_file in self.meta_dir.glob("*.meta.json"):
            try:
                metadata = load_metadata(meta_file)
                self.metadata_by_file.setdefault(metadata.file_name, metadata)
                self._scan_existing_blocks(metadata)
            except Exception as exc:
                self.log.warning("metadado ignorado %s: %s", meta_file, exc)

    def _setup_logger(self) -> logging.Logger:
        logger = logging.getLogger(f"peer-{self.peer_id}-{self.port}")
        logger.setLevel(logging.INFO)
        logger.handlers.clear()
        formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        file_handler = logging.FileHandler(self.logs_dir / f"{self.peer_id}.log", encoding=ENCODING)
        file_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)
        logger.propagate = False
        return logger

    def known_file_names(self) -> List[str]:
        with self.state_lock:
            return sorted(self.metadata_by_file.keys())

    def get_metadata(self, file_name: str) -> Optional[FileMetadata]:
        with self.state_lock:
            return self.metadata_by_file.get(file_name)

    def first_metadata(self) -> Optional[FileMetadata]:
        with self.state_lock:
            return next(iter(self.metadata_by_file.values()), None)

    def block_dir(self, file_name: str) -> Path:
        safe_name = file_name.replace(os.sep, "_")
        return self.blocks_root / safe_name

    def block_path(self, file_name: str, index: int) -> Path:
        return self.block_dir(file_name) / f"{index:08d}.part"

    def final_path(self, file_name: str) -> Path:
        return self.download_dir / file_name

    def _scan_existing_blocks(self, metadata: FileMetadata) -> None:
        """Registra blocos locais já verificados em memória.

        A verificação por hash é feita uma vez na inicialização; depois disso,
        o conjunto em memória evita releituras repetidas durante o download.
        """
        owned = self.verified_blocks.setdefault(metadata.file_name, set())
        directory = self.block_dir(metadata.file_name)
        if not directory.exists():
            return
        for idx in range(metadata.total_blocks):
            path = self.block_path(metadata.file_name, idx)
            if not path.exists():
                continue
            try:
                if sha256_bytes(path.read_bytes()) == metadata.block_sha256[idx]:
                    owned.add(idx)
            except OSError:
                pass

    def has_full_shared_file(self, file_name: str) -> bool:
        metadata = self.get_metadata(file_name)
        path = self.shared_dir / file_name
        return metadata is not None and path.exists() and path.stat().st_size == metadata.file_size

    def has_final_file(self, file_name: str) -> bool:
        metadata = self.get_metadata(file_name)
        path = self.final_path(file_name)
        if not (metadata and path.exists() and path.stat().st_size == metadata.file_size):
            return False
        return sha256_file(path) == metadata.file_sha256

    def have_block(self, file_name: str, index: int) -> bool:
        metadata = self.get_metadata(file_name)
        if not metadata or index < 0 or index >= metadata.total_blocks:
            return False
        if self.has_full_shared_file(file_name) or self.has_final_file(file_name):
            return True
        if index in self.verified_blocks.setdefault(file_name, set()):
            return True
        block_path = self.block_path(file_name, index)
        if not block_path.exists():
            return False
        try:
            data = block_path.read_bytes()
            if sha256_bytes(data) == metadata.block_sha256[index]:
                self.verified_blocks[file_name].add(index)
                return True
            return False
        except OSError:
            return False

    def have_bitfield(self, file_name: str) -> List[int]:
        metadata = self.get_metadata(file_name)
        if not metadata:
            return []
        if self.has_full_shared_file(file_name) or self.has_final_file(file_name):
            return [1] * metadata.total_blocks
        return [1 if self.have_block(file_name, idx) else 0 for idx in range(metadata.total_blocks)]

    def missing_blocks(self, file_name: str) -> List[int]:
        metadata = self.get_metadata(file_name)
        if not metadata:
            return []
        return [idx for idx in range(metadata.total_blocks) if not self.have_block(file_name, idx)]

    def read_block(self, file_name: str, index: int) -> Optional[bytes]:
        with self.state_lock:
            metadata = self.get_metadata(file_name)
            if not metadata or index < 0 or index >= metadata.total_blocks:
                return None
            if self.has_full_shared_file(file_name):
                path = self.shared_dir / file_name
                with path.open("rb") as stream:
                    stream.seek(index * metadata.block_size)
                    return stream.read(metadata.block_size)
            if self.has_final_file(file_name):
                path = self.final_path(file_name)
                with path.open("rb") as stream:
                    stream.seek(index * metadata.block_size)
                    return stream.read(metadata.block_size)
            path = self.block_path(file_name, index)
            if not path.exists():
                return None
            data = path.read_bytes()
            if sha256_bytes(data) != metadata.block_sha256[index]:
                self.log.warning("bloco local corrompido: arquivo=%s índice=%s", file_name, index)
                return None
            return data

    def store_block(self, file_name: str, index: int, data: bytes) -> bool:
        metadata = self.get_metadata(file_name)
        if not metadata:
            return False
        if index < 0 or index >= metadata.total_blocks:
            return False
        expected_hash = metadata.block_sha256[index]
        got_hash = sha256_bytes(data)
        if got_hash != expected_hash:
            self.log.warning("HASH_FAIL bloco=%s esperado=%s obtido=%s", index, expected_hash, got_hash)
            return False
        ensure_dir(self.block_dir(file_name))
        path = self.block_path(file_name, index)
        temp = path.with_suffix(".tmp")
        temp.write_bytes(data)
        temp.replace(path)
        self.verified_blocks.setdefault(file_name, set()).add(index)
        self.log.info("STORED bloco=%s bytes=%s arquivo=%s", index, len(data), file_name)
        return True

    def assemble_if_complete(self, file_name: str) -> bool:
        metadata = self.get_metadata(file_name)
        if not metadata:
            return False
        if self.has_full_shared_file(file_name) or self.has_final_file(file_name):
            return True
        bitfield = self.have_bitfield(file_name)
        if not bitfield or any(bit == 0 for bit in bitfield):
            return False
        ensure_dir(self.download_dir)
        temp = self.final_path(file_name).with_suffix(".tmp")
        with temp.open("wb") as output:
            for idx in range(metadata.total_blocks):
                output.write(self.block_path(file_name, idx).read_bytes())
        final = self.final_path(file_name)
        temp.replace(final)
        final_hash = sha256_file(final)
        if final.stat().st_size != metadata.file_size or final_hash != metadata.file_sha256:
            self.log.error("ASSEMBLE_FAIL arquivo=%s tamanho=%s/%s hash=%s/%s", file_name, final.stat().st_size, metadata.file_size, final_hash, metadata.file_sha256)
            return False
        self.log.info("ASSEMBLED arquivo=%s tamanho=%s sha256=%s", file_name, final.stat().st_size, final_hash)
        return True

    def request_json(self, neighbor: Tuple[str, int], message: Dict) -> Dict:
        sock = socket.create_connection(neighbor, timeout=self.socket_timeout)
        sock.settimeout(self.socket_timeout)
        channel = JsonLineSocket(sock)
        try:
            channel.send_json(message)
            return channel.read_json()
        finally:
            channel.close()

    def request_metadata_from_neighbors(self) -> Optional[FileMetadata]:
        target = self.target_file_name
        shuffled = list(self.neighbors)
        random.shuffle(shuffled)
        for neighbor in shuffled:
            try:
                response = self.request_json(neighbor, {"type": "META", "file_name": target, "version": PROTOCOL_VERSION, "peer_id": self.peer_id})
                if response.get("type") == "META_OK":
                    metadata = FileMetadata.from_dict(response["metadata"])
                    with self.state_lock:
                        self.metadata_by_file[metadata.file_name] = metadata
                        self.verified_blocks.setdefault(metadata.file_name, set())
                    self.target_file_name = metadata.file_name
                    save_metadata(metadata, self.meta_dir / f"{metadata.file_name}.meta.json")
                    self.log.info("META recebido de=%s:%s arquivo=%s blocos=%s", neighbor[0], neighbor[1], metadata.file_name, metadata.total_blocks)
                    return metadata
            except Exception as exc:
                self.log.info("META falhou vizinho=%s:%s erro=%s", neighbor[0], neighbor[1], exc)
        return None

    def request_have(self, neighbor: Tuple[str, int], file_name: str) -> Optional[List[int]]:
        try:
            response = self.request_json(neighbor, {"type": "HAVE", "file_name": file_name, "version": PROTOCOL_VERSION, "peer_id": self.peer_id})
            if response.get("type") == "HAVE_OK":
                bitfield = response.get("bitfield", [])
                if isinstance(bitfield, list):
                    return [1 if int(x) else 0 for x in bitfield]
        except Exception as exc:
            self.log.info("HAVE falhou vizinho=%s:%s arquivo=%s erro=%s", neighbor[0], neighbor[1], file_name, exc)
        return None

    def request_block(self, neighbor: Tuple[str, int], file_name: str, index: int) -> bool:
        return self.request_blocks(neighbor, file_name, [index]) == 1

    def request_blocks(self, neighbor: Tuple[str, int], file_name: str, indices: Sequence[int]) -> int:
        metadata = self.get_metadata(file_name)
        if not metadata or not indices:
            return 0
        sock = socket.create_connection(neighbor, timeout=self.socket_timeout)
        sock.settimeout(self.socket_timeout)
        channel = JsonLineSocket(sock)
        stored = 0
        try:
            channel.send_json({"type": "GET_MANY", "file_name": file_name, "indices": list(indices), "version": PROTOCOL_VERSION, "peer_id": self.peer_id})
            header = channel.read_json()
            if header.get("type") != "BLOCKS":
                self.log.info("GET_MANY falhou vizinho=%s:%s blocos=%s resposta=%s", neighbor[0], neighbor[1], len(indices), header.get("message"))
                return 0
            remote_peer_id = header.get("peer_id", "?")
            for item in header.get("items", []):
                index = int(item["index"])
                length = int(item["length"])
                data = channel.read_exact(length)
                if sha256_bytes(data) != str(item["sha256"]):
                    self.log.warning("BLOCK_HASH_FAIL transporte vizinho=%s:%s bloco=%s", neighbor[0], neighbor[1], index)
                    continue
                if self.store_block(file_name, index, data):
                    stored += 1
                    self.log.info("RECEIVED bloco=%s bytes=%s arquivo=%s from=%s:%s peer_remoto=%s", index, len(data), file_name, neighbor[0], neighbor[1], remote_peer_id)
            return stored
        except Exception as exc:
            self.log.info("GET_MANY erro vizinho=%s:%s blocos=%s erro=%s", neighbor[0], neighbor[1], len(indices), exc)
            return stored
        finally:
            channel.close()

    def start_server(self) -> None:
        self.server = ThreadedTCPServer((self.host, self.port), PeerRequestHandler)
        self.server.peer_node = self  # type: ignore[attr-defined]
        thread = threading.Thread(target=self.server.serve_forever, name=f"server-{self.peer_id}", daemon=True)
        thread.start()
        self.log.info("Servidor iniciado em %s:%s", self.host, self.port)

    def stop_server(self) -> None:
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            self.log.info("Servidor encerrado")

    def downloader_loop(self) -> None:
        if self.serve_only:
            self.log.info("Modo servidor: downloader desabilitado")
            return
        if not self.target_file_name and not self.metadata_by_file:
            self.log.info("Sem alvo definido; tentando obter metadados dos vizinhos")
        start = time.time()
        while not self.stop_event.is_set() and time.time() - start < self.max_runtime:
            file_name = self.target_file_name or (self.first_metadata().file_name if self.first_metadata() else None)
            metadata = self.get_metadata(file_name) if file_name else None
            if not metadata:
                metadata = self.request_metadata_from_neighbors()
                if not metadata:
                    time.sleep(0.3)
                    continue
                file_name = metadata.file_name
            assert file_name is not None

            if self.assemble_if_complete(file_name):
                self.log.info("Arquivo completo disponível: %s", file_name)
                if self.exit_when_complete:
                    self.stop_event.set()
                    return
                time.sleep(0.5)
                continue

            missing = self.missing_blocks(file_name)
            if not missing:
                time.sleep(self.request_interval)
                continue
            random.shuffle(missing)

            # Consulta disponibilidade nos vizinhos a cada rodada.
            availability: Dict[int, List[Tuple[str, int]]] = {idx: [] for idx in missing}
            shuffled_neighbors = list(self.neighbors)
            random.shuffle(shuffled_neighbors)
            for neighbor in shuffled_neighbors:
                bitfield = self.request_have(neighbor, file_name)
                if not bitfield:
                    continue
                for idx in missing:
                    if idx < len(bitfield) and bitfield[idx]:
                        availability[idx].append(neighbor)

            progressed = False
            max_blocks_per_round = 512
            batches: Dict[Tuple[str, int], List[int]] = {}
            # Estratégia rarest-first simplificada: blocos com menos fontes primeiro.
            candidate_blocks = sorted([idx for idx, peers in availability.items() if peers], key=lambda idx: len(availability[idx]))
            for idx in candidate_blocks:
                if self.stop_event.is_set() or self.have_block(file_name, idx):
                    continue
                peers = availability[idx]
                random.shuffle(peers)
                selected = peers[0]
                batches.setdefault(selected, []).append(idx)
                if sum(len(values) for values in batches.values()) >= max_blocks_per_round:
                    break

            for neighbor, indices in batches.items():
                if self.stop_event.is_set():
                    break
                received = self.request_blocks(neighbor, file_name, indices)
                if received > 0:
                    progressed = True
                    # Os blocos armazenados já ficam imediatamente disponíveis ao servidor deste peer.

            if not progressed:
                time.sleep(max(self.request_interval, 0.1))
        if not self.stop_event.is_set():
            self.log.warning("Tempo máximo atingido antes de concluir download")
            self.stop_event.set()

    def run(self) -> int:
        self.start_server()

        def handle_signal(signum, frame):  # type: ignore[no-untyped-def]
            self.log.info("Sinal recebido: %s", signum)
            self.stop_event.set()

        try:
            signal.signal(signal.SIGTERM, handle_signal)
            signal.signal(signal.SIGINT, handle_signal)
        except ValueError:
            pass

        downloader = threading.Thread(target=self.downloader_loop, name=f"downloader-{self.peer_id}", daemon=True)
        downloader.start()
        start = time.time()
        while not self.stop_event.is_set() and time.time() - start < self.max_runtime:
            time.sleep(0.2)
        self.stop_event.set()
        downloader.join(timeout=2.0)
        self.stop_server()
        file_name = self.target_file_name
        if file_name and self.get_metadata(file_name):
            complete = self.has_full_shared_file(file_name) or self.has_final_file(file_name)
            self.log.info("STATUS final arquivo=%s completo=%s blocos=%s", file_name, complete, sum(self.have_bitfield(file_name)))
            return 0 if complete or self.serve_only else 2
        return 0


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Peer P2P para transferência de arquivos por blocos.")
    parser.add_argument("--peer-id", required=True, help="Identificador textual do peer, ex.: A, B, C.")
    parser.add_argument("--host", default="127.0.0.1", help="Endereço de escuta do peer.")
    parser.add_argument("--port", type=int, required=True, help="Porta TCP de escuta do peer.")
    parser.add_argument("--neighbors", default="", help="Lista estática de vizinhos no formato host:porta,host:porta.")
    parser.add_argument("--data-dir", required=True, type=Path, help="Diretório de dados deste peer.")
    parser.add_argument("--file", type=Path, default=None, help="Arquivo de origem quando este peer é o seeder inicial.")
    parser.add_argument("--target", default=None, help="Nome do arquivo a baixar/servir.")
    parser.add_argument("--meta", type=Path, default=None, help="Arquivo .meta.json com metadados do arquivo.")
    parser.add_argument("--block-size", type=int, default=1024, help="Tamanho de bloco em bytes.")
    parser.add_argument("--timeout", type=float, default=DEFAULT_TIMEOUT, help="Timeout de socket em segundos.")
    parser.add_argument("--request-interval", type=float, default=0.05, help="Intervalo entre tentativas de download.")
    parser.add_argument("--max-runtime", type=float, default=60.0, help="Tempo máximo de execução do processo.")
    parser.add_argument("--exit-when-complete", action="store_true", help="Encerra o peer quando o arquivo alvo for concluído.")
    parser.add_argument("--serve-only", action="store_true", help="Mantém apenas o servidor ativo, sem baixar blocos.")
    parser.add_argument("--artificial-delay", type=float, default=0.0, help="Atraso artificial por bloco servido, útil para testes locais.")
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if args.file is not None and not args.file.is_file():
        parser.error(
            f"o arquivo informado em --file não existe: {args.file}. "
            "Informe um caminho válido para o arquivo que o seeder deve compartilhar."
        )
    if args.meta is not None and not args.meta.is_file():
        parser.error(
            f"o arquivo informado em --meta não existe: {args.meta}. "
            "Informe um caminho válido para o arquivo .meta.json."
        )
    node = PeerNode(
        peer_id=args.peer_id,
        host=args.host,
        port=args.port,
        neighbors=parse_neighbors(args.neighbors),
        data_dir=args.data_dir,
        source_file=args.file,
        target_file_name=args.target,
        metadata_path=args.meta,
        block_size=args.block_size,
        socket_timeout=args.timeout,
        request_interval=args.request_interval,
        max_runtime=args.max_runtime,
        exit_when_complete=args.exit_when_complete,
        serve_only=args.serve_only,
        artificial_delay=args.artificial_delay,
    )
    return node.run()


if __name__ == "__main__":
    raise SystemExit(main())
