# Development

The package is Goose with no C of its own; goose-hash, which it imports for SCRAM, has
a small C shim that `goose build` compiles. The modules that do I/O (`pg.net`,
`pg.socket`, `pg.eventloop`, `pg.pool`) bind the C library's socket calls by name when
the program starts and import no I/O library. The replayed tests have run on macOS on
arm64, before the package had its own I/O, and on Linux on x86-64 (Ubuntu under WSL 2),
the live tests against Postgres 18.4 in Docker (2026-11-12). The I/O modules are Linux's:
their tests and the live tests have run on Linux on x86-64 against Postgres 18.7 in
Docker (2026-11-17). See [Platforms](#platforms).

## Toolchain

Use the Goose pin in [README.md](../README.md#pins). The scripts also need:

| Tool | Requirement | Purpose |
|---|---|---|
| C compiler | clang or gcc; on Windows, MSVC or `clang-cl` with Visual Studio Build Tools | `goose build` of goose-hash's shim and each test |
| git | on `PATH` | `goose build` and `goose check` fetch goose-hash at its pin on first use |
| Python | 3, with its standard library | `test/run_tests.py`, `scripts/relay.py`, and `scripts/unix-forward.py` |
| POSIX shell, curl, shasum or sha256sum | on `PATH`; Git Bash on Windows | `scripts/goose.sh`, `scripts/test.sh`, and `scripts/live.sh` |
| Docker | with the `postgres:18` image, which it pulls | `scripts/live.sh` only |
| CMake | with Make or Ninja | building Goose from source, on Linux arm64 or for `GOOSE_COMMIT` |

## Set up the Goose compiler

The scripts use the Goose release pinned in `scripts/goose.sh` (`v2026.4`, 2026-11-12)
unless told otherwise. The first script that needs the compiler downloads the release's
archive for the platform from
[the releases page](https://github.com/aardappel/goose/releases), checks its SHA-256,
and unpacks it into `build/goose-v2026.4-<platform>/`; later runs reuse it. Run
`scripts/goose.sh` to fetch it and print the compiler's path. Linux on arm64 has no
release archive, so there the script builds the release's tag from source, as
`GOOSE_COMMIT` does below.

Use another compiler only when the release can't do the job, such as a Goose fix not
yet released. `GOOSE` names a binary; `GOOSE_COMMIT` names a full 40-hex commit of
`aardappel/goose`, which the scripts clone into `build/goose-<commit>/` and build with
CMake.

The release's `stdlib/` under `build/` needs no `exclude`: the repository has no
`.build` sidecar. One added later needs `exclude = ["build"]`, as goose-hash's has,
because Goose collects a package's symbol table from every `.goose` file under its root.

## Platforms

`net.goose` binds `libc.so.6` and uses Linux's constants (`SOCK_NONBLOCK`, `MSG_NOSIGNAL`,
the errno numbers) and its socket address layout (`sockaddr_in` and `sockaddr_un`, with a
2-byte family). It passes a slice to `send`, `recv` and `poll` where C takes a pointer
and a length: a slice crosses as its pointer and length, which are the two registers C
reads, on the 64-bit Linux ABIs. macOS has other constants, a length byte in its
address layout, and `libc.dylib`; Windows has none of it. A port is `net.goose` alone,
since the loop, the socket and the pool call nothing else.

## Test

`scripts/test.sh` runs every test that needs no server, and stops at the first step
that fails:

1. `goose check` of `pg.goose` alone, of each test helper under `test/lib/`, and of
   each program under `test/live/`.
2. `test/run_tests.py`: the tests under `test/<area>/`, each built with `goose build`
   at `-O0` and `-O2` and run, its output checked against `test/expected/<name>.out`.

```sh
scripts/test.sh
python3 test/run_tests.py session          # one area
python3 test/run_tests.py --bless arena    # rewrite test/expected/arena.out
```

The areas:

- `test/messages/`: the messages the client writes, byte for byte against the
  protocol's layouts, and the server's that recorded sessions lack: messages cut at
  any point, several in one read, an error at a sync, a message at
  `Config.max_message` and one a byte over, and each way a connection fails, from MD5, a cleartext request not allowed, and an iteration count over the cap, to a
  wrong SCRAM signature, a client_encoding other than UTF8, and a server that breaks
  the protocol; strings with a NUL byte, which the writers refuse, and a message a
  byte longer than `MAX_MESSAGE`, which they refuse too; and reading a field past a
  row's last. That message's parameter, and the one `refused` tries to send, take
  1 GB of memory in each run, at `-O0` and at `-O2`.
- `test/scram/`: RFC 7677's example exchange, the server-first-messages the client
  refuses, and a cleartext request after a completed SCRAM, built from `arena`'s
  capture (`after_scram`).
- `test/url/`: `postgres://` URLs, with NUL bytes and `sslmode`.
- `test/session/`: whole sessions with a server, replayed from captures
  ([below](#session-tests)): HttpArena's `/async-db` query in both formats
  (`arena`, and `arena_bytewise`, which hands the server's bytes over one at a time),
  every decoded type and each outcome a pipeline's entry can have (`pipeline`), a
  cleartext login, allowed (`cleartext`) and refused by default
  (`cleartext_refused`), a wrong password (`bad_password`), and the blocking facade's
  calls, errors, and savepoints (`facade`), and its login (`facade_login`); and
  `pg::transaction` (`transaction`): commits, rollbacks on `pg::fail` two calls down
  and on a failed statement, a block that catches an error itself, in a statement and
  in a row block, and so fails its `COMMIT`, nested savepoints that each undo only
  their own work, a transaction begun by `exec`, an inner block that ends the whole
  transaction, and, last, a connection the server ends inside an inner block.
- `test/facade/`: the facade against a server `test/lib/fake/fake.goose` fakes in the program,
  for what a capture can't show: a connection lost in a result, a FATAL error, a send
  that fails, a row at `Config.max_message`, one over it, and one over the default
  of which only the head arrives, the statement cache's limits and statements the server dropped, and
  what the facade refuses before sending, more than 65535 parameters and a message
  longer than `MAX_MESSAGE` among it (`refused`); and a `pg::Params` list as `Bind` writes it, up to 65535 parameters,
  and the `Bind` that `execute` refuses past them (`params`); and a transaction whose
  connection is lost in its block, or whose `COMMIT` can't be sent
  (`transaction_broken`).
- `test/compile/`: `// error:` fixtures, programs that must not compile: a facade call
  outside `pg::run`, `pg::fail` and a `pg::transaction` outside one, a query inside another's row
  block.
- `test/net/`: the package's own I/O over loopback and Unix sockets, with no server:
  `sockets` (addresses, a listener on port 0, a non-blocking connect, bytes both ways,
  a refused connect), `loop_echo` (a server and a client on one loop, a send that fills
  the loop's buffer and finishes through `Writable`, a refused dial, a dial to text that
  is no address, `stop`), `loop_unix` (a Unix listener, a second at the same path that
  fails, and the path removed when it ends), and `pool_loop` (the pool against a
  Postgres server scripted in Goose on the same loop: a login, rows, a request that
  waits in the queue and gets the server's error, a refused server, a host name, a
  server that never answers, and another library on the loop that takes only its own
  events), `loop_step` (the loop driven one pass at a time by a host loop), and
  `pool_host` (the pool on a toy host that is not the package's loop: it imports
  `pg.pool` and no loop, and a scripted Postgres answers what the pool sends).

`test/run_tests.py` skips `test/live/`, whose programs need a server:
`scripts/live.sh` runs them ([below](#the-socket-test)).

### Session tests

A session test is a program that talks to a server through `test/lib/wire/wire.goose`. Under
`test/run_tests.py` it replays a capture from `test/data/`: it gets the server's
recorded bytes when it asks for input, and each message it sends must equal the
recorded one, or it aborts. SCRAM's client nonce is fixed in the tests, so the client's
proof in a capture replays.

`scripts/live.sh` runs the same programs against a server, and checks that each prints
its `test/expected/` file, so a replay and a live run agree:

```sh
scripts/live.sh             # against a new container on port 55432
PG_PORT=55433 scripts/live.sh
scripts/live.sh --record    # also write test/data/*.cap and test/expected/*.out
scripts/live.sh --record facade    # only the tests named
```

It starts the `postgres:18` image as HttpArena does: user `bench`, password `bench`,
database `benchmark`, seeded with the 100,000 rows of HttpArena's `data/pgdb-seed.sql`
at `2d85137`, which it fetches into `build/arena/` and checks against its SHA-256. The
image authenticates TCP connections with SCRAM-SHA-256. `scripts/live-init.sh` adds a
user `plain` with a `password` line in `pg_hba.conf`, for the cleartext test. The
container's data is on a tmpfs, and the script removes the container when it ends.

The session programs do no I/O themselves, so `scripts/relay.py` connects each program to
the server: it starts the program with `--live`, sends what the program writes as
`>hex` lines, answers each `<` line with the server's next bytes in hex, and passes the
other lines through as the test's output. With `--record` it writes the capture. The
facade's tests reach the same relay through `wire::Io`'s transport overloads.

Record again after changing what a session sends, or to move to a new Postgres version,
and read the diff of `test/expected/` before committing: the server's nonce, salt, and
backend key change in every capture, and nothing a test prints may.

### The socket test

`scripts/live.sh` also runs `test/live/socket.goose`, the facade over the package's
socket with no relay: connects that fail (refused, a missing Unix socket, a host name,
which nothing resolves, and a URL that isn't one), then the
session `test/session/facade.goose` replays, through the same `wire::facade`, and
`pg::open` on a connection again. It runs twice, over
TCP to the container's port and over a Unix socket, and both runs must print
`test/expected/socket.out`. `--record` rewrites that file from the TCP run; it has no
capture.

Docker Desktop can't share a Unix socket between its VM and the host: Postgres fails
to start with "could not create any Unix-domain sockets" when its socket directory is
a bind mount (macOS, 2026-10-19). So the Unix run connects to a socket that
`scripts/unix-forward.py` serves in a directory under `/tmp`, which forwards each
connection to the container's TCP port. The run tests the client's side, `pg::open`'s
`unix:` path and the socket's Unix connect, against a real server, while Postgres
itself sees TCP.

### The pool test

`scripts/live.sh` runs `test/live/pool.goose` last, with no relay: the program runs the
package's event loop and a pool against the container. One request; 40 requests at once
on four connections, each asking for a different number of rows and checked against its
own; the same 40 on one connection, so all but one wait in the queue; a request that
fails with the server's error (`42P01`); and a server that is not there. It must print
`test/expected/pool.out`, which `--record` rewrites. The cases that stress the server
(a query past its time limit, an answer longer than `POOL_INPUT`, a server that ends every
session) run on the scripted server of `test/net/pool_loop.goose`, or are still to be
written (task 005).

## Traps

- A fixed-size value passed to an untyped parameter is copied: `decode_into(cfg.user,
  ...)` with `fn decode_into(out, ...)` appended to a copy of the field, and the
  startup message went out without a user. Take such a parameter as `out: B&`.
- `null` is a keyword, so the NULL parameter is `pg::param_null`, and the facade's is
  `pg::NULL`.
- A transport's overloads are declared from another file as `fn pg::send(t:
  wire::Io&, ...)`. The qualifier puts the whole declaration in namespace `pg`, its
  body included, so the body names its own namespace's functions in full
  (`wire::recv`).
- `for e in pg::run() { ... } { ... }` doesn't parse as a loop over the errors: the
  second block is taken as the loop's body and the first as `run`'s missing argument.
  Bind the errors first, `let failed = pg::run() { ... };`.
- A module named `loop` can't be imported: `loop` is a keyword, so the event loop is
  `eventloop.goose`, imported as `pg.eventloop`.
- A generic function is given its block last: `lp.poll_once(immediate, F)`, with
  `fn poll_once<F>(lp: Loop&, immediate: bool)`.
- A shift of a `u8` happens in `u8`, so `p[1] << 8` loses the byte; widen first,
  `(p[1] as i64) << 8`. `errno` and the bound port read a wrong value otherwise.
- A Unix socket path is at most 107 bytes. `unix:` addresses longer than that fail as
  addresses.
- A block's parameters are taken by value, and a growable array isn't copied
  implicitly, so a block can't take the output buffer itself: the pool's parameter
  writer gets a `Binding`, a struct holding a reference to it.
- A function in `socket.goose` that names a type of `pg.goose` in its signature,
  such as `Config`, fails `goose check socket.goose` on the file alone; an untyped
  parameter is checked only where it's called.
- Docker Desktop's port forwarding passes on a backend's last bytes, and its close,
  a little after `pg_stat_activity` shows the backend gone (macOS, 2026-10-23), so a
  live test that ends the server's sessions waits a little for the close.
