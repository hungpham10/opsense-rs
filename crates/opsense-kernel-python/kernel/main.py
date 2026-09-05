#!/usr/bin/env python3
"""Opsense Python analysis kernel.

Speaks the framed stdio protocol from opsense-proto: CONTROL frames carry
protobuf `Envelope`s (vendored under gen/), ARROW frames carry one Arrow IPC
stream segment each. Layout mirrors the echo reference kernel so the harness
tests stay identical; plain code runs as real Python.

Threading model: a background reader thread parses stdin frames into a queue;
the MAIN thread executes user code so SIGINT (host interrupt) raises
KeyboardInterrupt exactly where the work happens.
"""

import io
import importlib.util
import os
import queue
import resource
import signal
import sys
import threading
import time
from contextlib import redirect_stdout

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from gen.opsense.kernel.v1 import opsense_pb2 as pb  # noqa: E402

PROTO_VERSION = 1
KERNEL_NAME = "python"
TAG_CONTROL, TAG_ARROW = 1, 2

DEFAULT_PACKAGES = ["numpy", "pandas", "pyarrow", "scipy", "sklearn",
                    "statsmodels", "matplotlib"]
REQUIRED_PACKAGES = ["numpy", "pandas", "pyarrow", "google.protobuf"]

arrows = []  # buffered Arrow segments, appended by the reader thread
requests = queue.Queue()
state = {"executing": False}
# Capture real stdout for protocol writes (redirect_stdout replaces sys.stdout
# with LiveStdout during exec — writing protocol frames there would recurse).
_real_stdout = sys.stdout.buffer


def _sigint_handler(_signum, _frame):
    if state["executing"]:
        raise KeyboardInterrupt()
    # idle: ignore — interrupts only matter while code runs


signal.signal(signal.SIGINT, _sigint_handler)


def read_exact(stream, n):
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            raise EOFError
        buf.extend(chunk)
    return bytes(buf)


def reader():
    stdin = sys.stdin.buffer
    try:
        while True:
            header = read_exact(stdin, 5)
            tag = header[0]
            length = int.from_bytes(header[1:5], "big")
            payload = read_exact(stdin, length)
            if tag == TAG_CONTROL:
                envelope = pb.Envelope()
                envelope.ParseFromString(payload)
                requests.put(("control", envelope))
            elif tag == TAG_ARROW:
                arrows.append(payload)
    except EOFError:
        requests.put(("eof", None))


def send(envelope):
    data = envelope.SerializeToString()
    frame = bytes([TAG_CONTROL]) + len(data).to_bytes(4, "big") + data
    _real_stdout.write(frame)
    _real_stdout.flush()


def event(request_id, field, value):
    ev = pb.ExecEvent(request_id=request_id)
    setattr(ev, field, value)
    return ev


def send_event(request_id, field, value):
    ev = pb.ExecEvent(request_id=request_id)
    # Scalar fields use setattr; message fields need explicit sub-message access.
    if field in ("error", "dataframe", "result_value"):
        sub = getattr(ev, field)
        sub.CopyFrom(value)
    else:
        setattr(ev, field, value)
    send(pb.Envelope(exec_event=ev))


def ack(ok=True, error=""):
    send(pb.Envelope(ack=pb.Ack(ok=ok, error=error)))


class LiveStdout(io.TextIOBase):
    """stdout sink forwarding every line as an ExecEvent immediately."""

    def __init__(self, request_id):
        self.request_id = request_id
        self._buf = ""

    def write(self, text):
        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="replace")
        print(f"[DEBUG LiveStdout.write] {text!r}", file=sys.stderr)
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line:
                send_event(self.request_id, "stdout_line", line)
        return len(text)

    def flush(self):
        pass

    @property
    def buffer(self):
        return self  # satisfy code that accesses .buffer

    def isatty(self):
        return False

    def writable(self):
        return True

    def readable(self):
        return False

    def seekable(self):
        return False


def probe_packages(names):
    import importlib.metadata as importlib_metadata
    infos = []
    for name in names:
        spec = importlib.util.find_spec(name)
        version = ""
        available = spec is not None
        if available:
            try:
                version = importlib_metadata.version(
                    "protobuf" if name == "google.protobuf" else name)
            except Exception:
                pass
        infos.append(pb.PackageInfo(name=name, available=available,
                                     version=version))
    return infos


class Session:
    def __init__(self, params):
        self.globals = {"__name__": "__opsense__"}
        self.datasets = {}
        self.last_ref = None
        self.allow_fs = params.allow_fs
        self.setup(params)

    def setup(self, params):
        # Heavy imports BEFORE restrictions (they read their own data files).
        for name in ["numpy", "pandas", "pyarrow"]:
            importlib.import_module(name)

        # Bundled opsense_* helper modules from the asset dir.
        # Tolerant: a module that imports a missing package is skipped with
        # a warning — the kernel still works for everything else.
        for fname in sorted(os.listdir(HERE)):
            if not (fname.startswith("opsense_") and fname.endswith(".py")):
                continue
            mod_name = fname[:-3]
            spec = importlib.util.spec_from_file_location(
                mod_name, os.path.join(HERE, fname))
            module = importlib.util.module_from_spec(spec)
            try:
                sys.modules[mod_name] = module
                spec.loader.exec_module(module)
                self.globals[mod_name] = module
            except (ImportError, ModuleNotFoundError) as exc:
                del sys.modules[mod_name]
                print(f"[kernel] skipping {mod_name}: {exc}", file=sys.stderr)

        # Sandbox policy last.
        import builtins
        import socket as socket_mod

        if not params.allow_fs:
            def _blocked(*_a, **_k):
                raise PermissionError(
                    "filesystem access is disabled in this session "
                    "(set allow_fs to enable)")
            builtins.open = _blocked

        if not params.allow_net:
            def _blocked(*_a, **_k):
                raise PermissionError(
                    "network access is disabled in this session "
                    "(set allow_net to enable)")
            socket_mod.socket = _blocked
            socket_mod.create_connection = _blocked
            socket_mod.getaddrinfo = _blocked

        if params.max_memory_mb > 0:
            try:
                cap = int(params.max_memory_mb) * 1024 * 1024
                resource.setrlimit(resource.RLIMIT_AS, (cap, cap))
            except Exception:
                pass  # platform refuses soft AS caps (e.g. macOS)


def classify(value):
    """Captured `result` -> pb.Value (DataFrame via Arrow, else repr text)."""
    import pandas as pd
    import pyarrow as pa

    table = None
    if isinstance(value, pa.Table):
        table = value
    elif isinstance(value, pa.RecordBatch):
        table = pa.Table.from_batches([value])
    elif isinstance(value, pd.DataFrame):
        table = pa.Table.from_pandas(value, preserve_index=False)
    if table is not None:
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as writer:
            writer.write_table(table)
        return pb.Value(dataframe=pb.DataFrame(
            arrow_ipc=sink.getvalue().to_pybytes(),
            rows=table.num_rows,
            cols=table.num_columns,
            columns=list(table.column_names),
        ))
    return pb.Value(text=repr(value))


def load_dataset(session, ref):
    """Concatenate buffered ARROW segments into one table stored under ref."""
    import pyarrow as pa

    segments, arrows[:] = list(arrows), []
    tables = []
    for segment in segments:
        reader = pa.ipc.open_stream(segment)
        tables.append(reader.read_all())
    table = pa.concat_tables(tables) if len(tables) > 1 else tables[0]
    session.datasets[ref] = table
    session.last_ref = ref
    return table


def exec_code(session, req):
    code = req.code.strip()
    events = []

    interrupted = False
    error = None
    result_value = None

    state["executing"] = True
    try:
        if code.startswith("sleep:"):
            time.sleep(max(int(code[6:].strip()), 0) / 1000.0)
        elif code.startswith("print:"):
            events.append(("stdout_line", code[6:]))
        elif code.startswith("err:"):
            kind, _, message = code[4:].partition(":")
            events.append(("error", pb.ErrorEvent(kind=kind or "error",
                                                   message=message)))
        elif code == "df":
            if not session.last_ref:
                raise RuntimeError(
                    "no dataset received yet - send one first")
            result_value = classify(session.datasets[session.last_ref])
        else:
            # Auto-inject ALL datasets as pandas DataFrames with Python-safe
            # names: "@1" → "_df_1". Users write `_df_1['col']` in their code.
            for ds_name, table in session.datasets.items():
                safe = ds_name.replace("@", "_df_")
                if safe not in session.globals:
                    session.globals[safe] = table.to_pandas()
            # Capture stdout into a StringIO — simpler and more reliable than
            # live-streaming (avoids redirect_stdout recursion issues).
            captured = io.StringIO()
            with redirect_stdout(captured):
                exec(compile(code, "<opsense>", "exec"), session.globals)
            # Send captured lines as stdout events.
            for line in captured.getvalue().splitlines():
                if line:
                    events.append(("stdout_line", line))
            if "result" in session.globals and \
                    session.globals["result"] is not None:
                result_value = classify(session.globals.pop("result"))
    except KeyboardInterrupt:
        interrupted = True
    except Exception as exc:  # noqa: BLE001 - root cause goes to the host
        error = f"{type(exc).__name__}: {exc}"
    finally:
        state["executing"] = False

    if interrupted:
        events.append(("error", pb.ErrorEvent(
            kind="cancelled", message="interrupted by host")))
    elif error is not None:
        events.append(("error", pb.ErrorEvent(kind="python_exception",
                                               message=error)))
    if result_value is not None:
        events.append(("result_value", result_value))
    events.append(("done", True))
    for field, value in events:
        send_event(req.request_id, field, value)


def handle(envelope, sessions):
    msg = envelope.WhichOneof("msg")
    if msg == "hello":
        if envelope.hello.protocol_version != PROTO_VERSION:
            sys.stderr.write(
                f"protocol mismatch: host v{envelope.hello.protocol_version}\n")
            return False
        send(pb.Envelope(welcome=pb.Welcome(
            protocol_version=PROTO_VERSION, kernel_name=KERNEL_NAME,
            kernel_version=os.environ.get("OPSENSE_KERNEL_VERSION", "0.1.0"))))
    elif msg == "health_request":
        packages = probe_packages(DEFAULT_PACKAGES)
        missing = [p.name for p in packages if p.name in REQUIRED_PACKAGES
                   and not p.available]
        send(pb.Envelope(health_status=pb.HealthStatus(
            ok=not missing,
            kernel_name=KERNEL_NAME,
            kernel_version=os.environ.get("OPSENSE_KERNEL_VERSION", "0.1.0"),
            packages=packages,
            detail="missing: " + ",".join(missing) if missing else "ready")))
    elif msg == "start_session":
        sid = envelope.start_session.session_id
        sessions[sid] = Session(envelope.start_session)
        send(pb.Envelope(session_handle=pb.SessionHandle(session_id=sid)))
    elif msg == "close_request":
        sessions.pop(envelope.close_request.session_id, None)
        ack()
    elif msg == "dataset_header":
        header = envelope.dataset_header
        session = sessions.get(header.session_id)
        if session is None:
            send(pb.Envelope(dataset_ack=pb.DatasetAck(
                dataset_ref=header.dataset_ref, ok=False,
                error=f"unknown session {header.session_id}")))
            return True
        table = load_dataset(session, header.dataset_ref)
        send(pb.Envelope(dataset_ack=pb.DatasetAck(
            dataset_ref=header.dataset_ref, rows=table.num_rows, ok=True)))
    elif msg == "code_request":
        req = envelope.code_request
        session = sessions.get(req.session_id)
        if session is None:
            send_event(req.request_id, "error", pb.ErrorEvent(
                kind="bad_request",
                message=f"unknown session {req.session_id}"))
            send_event(req.request_id, "done", True)
            return True
        exec_code(session, req)
    elif msg == "interrupt_request":
        # Delivered while idle -> nothing to cancel; acknowledge directly.
        # While executing, the reader thread raises SIGINT into the main
        # thread and the cancellation surfaces on the execute stream instead.
        if not state["executing"]:
            ack()
        else:
            signal.raise_signal(signal.SIGINT)
    elif msg == "shutdown":
        ack()
        return False
    else:
        ack(False, f"unexpected envelope: {msg}")
    return True


def main():
    # The reader parses frames sequentially, so by the time a DatasetHeader
    # control reaches this queue its ARROW frames are already buffered.
    threading.Thread(target=reader, daemon=True).start()
    sessions = {}
    while True:
        kind, payload = requests.get()
        if kind == "eof":
            return 0
        if kind == "control" and not handle(payload, sessions):
            return 0


if __name__ == "__main__":
    sys.exit(main())
