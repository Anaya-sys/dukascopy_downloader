"""
Benchmark mínimo para la Teoría 1.

No depende de la red de Dukascopy: levanta un servidor HTTP local que
simula la latencia de un servidor remoto (RTT ~150ms, típico de un
fetch a un CDN/servidor europeo desde otra región). Compara:

  - Secuencial: un único httpx.Client haciendo N requests uno a uno
    (lo que propone la teoría 1 como alternativa).
  - Paralelo:   ThreadPoolExecutor(max_workers=8), el patrón real de
    orchestrator.py.

La pregunta NO es "¿qué tan rápido es Dukascopy?" sino "¿el patrón
ThreadPoolExecutor + httpx síncrono logra solapar la espera de I/O?".
Esa propiedad depende del runtime de Python (GIL liberado durante I/O),
no del servidor remoto específico, así que el resultado generaliza.
"""

import http.server
import socketserver
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

LATENCY = 0.15  # 150 ms por request, simulando latencia de red real
N = 40          # nº de chunks a "descargar"
WORKERS = 16     # mismo valor que config.MAX_WORKERS


class SlowHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        time.sleep(LATENCY)
        body = b"x" * 2048
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *a):
        pass


def main():
    server = socketserver.ThreadingTCPServer(("127.0.0.1", 0), SlowHandler)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()

    url = f"http://127.0.0.1:{port}/chunk"

    # ── Secuencial ──────────────────────────────────────────────────────
    client = httpx.Client()
    t0 = time.perf_counter()
    for _ in range(N):
        r = client.get(url)
        assert r.status_code == 200
    t_seq = time.perf_counter() - t0
    client.close()

    # ── Paralelo (igual que orchestrator.py: ThreadPoolExecutor + httpx) ─
    client2 = httpx.Client()
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        results = list(pool.map(lambda _: client2.get(url), range(N)))
    t_par = time.perf_counter() - t0
    assert all(r.status_code == 200 for r in results)
    client2.close()

    server.shutdown()

    print(f"N = {N} chunks, latencia simulada = {LATENCY*1000:.0f} ms/req, workers = {WORKERS}")
    print(f"Secuencial (1 request a la vez): {t_seq:6.2f} s")
    print(f"Paralelo  ({WORKERS} workers)        : {t_par:6.2f} s")
    print(f"Speedup observado               : {t_seq / t_par:5.1f}x")
    print(f"Teórico (límite superior)       : {WORKERS:5.0f}x")


if __name__ == "__main__":
    main()