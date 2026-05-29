#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Executor de estudos de caso locais para o trabalho P2P."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Tuple

ENCODING = "utf-8"
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while True:
            chunk = stream.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def create_deterministic_file(path: Path, size_bytes: int, seed: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    remaining = size_bytes
    with path.open("wb") as output:
        while remaining > 0:
            chunk_size = min(1024 * 1024, remaining)
            # Conteúdo pseudoaleatório determinístico, evitando dependência de os.urandom.
            data = bytearray(rng.getrandbits(8) for _ in range(chunk_size))
            output.write(data)
            remaining -= chunk_size


def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_port(port: int, timeout: float = 8.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


@dataclass
class TestCase:
    name: str
    peer_count: int
    block_size: int
    file_size: int
    max_runtime: float = 45.0


@dataclass
class TestResult:
    name: str
    peer_count: int
    block_size: int
    file_size: int
    total_blocks: int
    duration_seconds: float
    checksum_ok: bool
    size_ok: bool
    completed_peers: int
    remote_sources: Dict[str, int]
    status: str


def parse_sources(log_path: Path) -> Dict[str, int]:
    sources: Dict[str, int] = {}
    if not log_path.exists():
        return sources
    for line in log_path.read_text(encoding=ENCODING, errors="replace").splitlines():
        if "RECEIVED" in line and "peer_remoto=" in line:
            remote = line.rsplit("peer_remoto=", 1)[-1].strip()
            sources[remote] = sources.get(remote, 0) + 1
    return sources


def terminate_processes(processes: List[subprocess.Popen]) -> None:
    for process in processes:
        if process.poll() is None:
            process.terminate()
    for process in processes:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def run_case(case: TestCase, output_dir: Path, verbose: bool = False) -> TestResult:
    case_dir = output_dir / case.name
    if case_dir.exists():
        shutil.rmtree(case_dir)
    case_dir.mkdir(parents=True)
    source_dir = case_dir / "source"
    source_file = source_dir / f"{case.name}.bin"
    create_deterministic_file(source_file, case.file_size, seed=case.file_size + case.block_size + case.peer_count)
    original_hash = sha256_file(source_file)

    ports = [find_free_port() for _ in range(case.peer_count)]
    peer_ids = [chr(ord("A") + idx) for idx in range(case.peer_count)]
    all_neighbors = []
    for idx in range(case.peer_count):
        neighbors = [f"127.0.0.1:{port}" for j, port in enumerate(ports) if j != idx]
        all_neighbors.append(",".join(neighbors))

    processes: List[subprocess.Popen] = []
    start = time.time()
    try:
        # Seeder inicial A.
        seeder_dir = case_dir / "peer_A"
        seeder_cmd = [
            sys.executable, "-m", "p2p_transfer.peer",
            "--peer-id", "A",
            "--host", "127.0.0.1",
            "--port", str(ports[0]),
            "--neighbors", all_neighbors[0],
            "--data-dir", str(seeder_dir),
            "--file", str(source_file),
            "--target", source_file.name,
            "--block-size", str(case.block_size),
            "--serve-only",
            "--max-runtime", str(case.max_runtime),
            "--artificial-delay", "0.0002" if case.peer_count >= 4 else "0.0",
        ]
        seeder_out = (case_dir / "peer_A_stdout.log").open("w", encoding=ENCODING)
        processes.append(subprocess.Popen(seeder_cmd, cwd=PROJECT_ROOT, stdout=seeder_out, stderr=subprocess.STDOUT, text=True))
        if not wait_for_port(ports[0]):
            raise RuntimeError("Seeder A não abriu a porta TCP")

        # Metadado gerado pelo seeder.
        meta_path = seeder_dir / "metadata" / f"{source_file.name}.meta.json"
        deadline = time.time() + 5
        while not meta_path.exists() and time.time() < deadline:
            time.sleep(0.05)
        if not meta_path.exists():
            raise RuntimeError("Metadado não foi criado pelo seeder")

        # Leechers B..N. Eles também sobem servidor; ao receber um bloco já podem servi-lo.
        for idx in range(1, case.peer_count):
            peer_id = peer_ids[idx]
            data_dir = case_dir / f"peer_{peer_id}"
            cmd = [
                sys.executable, "-m", "p2p_transfer.peer",
                "--peer-id", peer_id,
                "--host", "127.0.0.1",
                "--port", str(ports[idx]),
                "--neighbors", all_neighbors[idx],
                "--data-dir", str(data_dir),
                "--meta", str(meta_path),
                "--target", source_file.name,
                "--block-size", str(case.block_size),
                "--exit-when-complete",
                "--max-runtime", str(case.max_runtime),
                "--request-interval", "0.01",
                "--timeout", "5",
            ]
            out = (case_dir / f"peer_{peer_id}_stdout.log").open("w", encoding=ENCODING)
            processes.append(subprocess.Popen(cmd, cwd=PROJECT_ROOT, stdout=out, stderr=subprocess.STDOUT, text=True))
            if not wait_for_port(ports[idx]):
                raise RuntimeError(f"Peer {peer_id} não abriu a porta TCP")

        # Aguarda leechers terminarem. O seeder fica ativo até ser finalizado.
        deadline = time.time() + case.max_runtime
        while time.time() < deadline:
            leecher_done = all(process.poll() is not None for process in processes[1:])
            if leecher_done:
                break
            time.sleep(0.2)
        duration = time.time() - start
    finally:
        terminate_processes(processes)

    completed_peers = 0
    checksum_ok = True
    size_ok = True
    sources: Dict[str, int] = {}
    for idx in range(1, case.peer_count):
        peer_id = peer_ids[idx]
        data_dir = case_dir / f"peer_{peer_id}"
        downloaded = data_dir / "downloads" / source_file.name
        if downloaded.exists():
            this_size_ok = downloaded.stat().st_size == case.file_size
            this_hash_ok = sha256_file(downloaded) == original_hash
            if this_size_ok and this_hash_ok:
                completed_peers += 1
            size_ok = size_ok and this_size_ok
            checksum_ok = checksum_ok and this_hash_ok
        else:
            size_ok = False
            checksum_ok = False
        peer_sources = parse_sources(data_dir / "logs" / f"{peer_id}.log")
        for key, value in peer_sources.items():
            sources[key] = sources.get(key, 0) + value

    total_blocks = (case.file_size + case.block_size - 1) // case.block_size
    status = "OK" if checksum_ok and size_ok and completed_peers == case.peer_count - 1 else "FALHA"
    result = TestResult(
        name=case.name,
        peer_count=case.peer_count,
        block_size=case.block_size,
        file_size=case.file_size,
        total_blocks=total_blocks,
        duration_seconds=round(duration, 3),
        checksum_ok=checksum_ok,
        size_ok=size_ok,
        completed_peers=completed_peers,
        remote_sources=sources,
        status=status,
    )
    (case_dir / "resultado.json").write_text(json.dumps(asdict(result), indent=2, ensure_ascii=False), encoding=ENCODING)
    if verbose:
        print(json.dumps(asdict(result), indent=2, ensure_ascii=False))
    return result


def default_cases() -> List[TestCase]:
    return [
        TestCase(name="fileA_10KB_2peers_1KB", peer_count=2, block_size=1024, file_size=10 * 1024, max_runtime=20),
        TestCase(name="fileA_20KB_2peers_4KB", peer_count=2, block_size=4096, file_size=20 * 1024, max_runtime=20),
        TestCase(name="fileB_1MB_2peers_1KB", peer_count=2, block_size=1024, file_size=1 * 1024 * 1024, max_runtime=45),
        TestCase(name="fileB_5MB_4peers_4KB", peer_count=4, block_size=4096, file_size=5 * 1024 * 1024, max_runtime=70),
        TestCase(name="fileC_10MB_2peers_4KB", peer_count=2, block_size=4096, file_size=10 * 1024 * 1024, max_runtime=90),
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Executa estudos de caso do P2P localmente.")
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "test_runs", help="Diretório para massas de teste e logs.")
    parser.add_argument("--quick", action="store_true", help="Executa apenas dois testes pequenos.")
    parser.add_argument("--verbose", action="store_true", help="Imprime resultados individuais.")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    cases = default_cases()[:2] if args.quick else default_cases()
    results = [run_case(case, args.output_dir, verbose=args.verbose) for case in cases]
    summary_path = args.output_dir / "summary.json"
    summary_path.write_text(json.dumps([asdict(result) for result in results], indent=2, ensure_ascii=False), encoding=ENCODING)
    print(json.dumps([asdict(result) for result in results], indent=2, ensure_ascii=False))
    return 0 if all(result.status == "OK" for result in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
