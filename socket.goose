// A blocking socket as the facade's transport, for a thread that runs no event
// loop, such as a Plod lane. It is the package's own (net.goose): a program
// that imports this module needs no I/O library.
//
//   import pg from "github.com/kaitk/goose-postgres@<tag>";
//   import pg.socket;
//
//   var db = pg::Db { t: pg::Socket { .. }, .. };
//   let failed = pg::run() {
//       pg::open(db, cfg, 5000);     // the socket, then the session
//       pg::query(db, ...) { r => ... };
//   };
//   pg::close(db);
//
// Each wait, the connect, a send, or a receive, lasts at most the limit
// given; one that runs out fails the call and breaks the connection.
namespace pg;

import std;
import .net;

// The bytes a receive makes room for at once.
let RECEIVE_CHUNK = 16384;

// A connection to the server, and the limit in milliseconds on each wait
// (-1: none). Build it empty, `pg::Socket { .. }`, and let open connect it.
struct Socket { fd: i64 = -1, ms: i64 = -1 }

// Waits until fd is ready for events, for at most ms (-1: no limit). False if
// the time ran out or the wait failed.
fn ready_within(fd: i64, events: i16, ms: i64) -> bool {
    var fds: PollFd[1] = [PollFd { fd: fd as i32, events: events, .. }];
    let start = clock_ms();
    loop {
        let left = if ms < 0 { -1 } else { max(ms - (clock_ms() - start), 0) };
        let n = wait_ready(fds[..], left);
        if n > 0 { return true; }
        if n < 0 || (ms >= 0 && clock_ms() - start >= ms) { return false; }
    }
}

fn pg::send(t: pg::Socket&, bytes: const u8[:]) -> bool {
    guard t.fd >= 0 else { return false; }
    var at = 0;
    while at < bytes.len {
        let n = send_some(t.fd, bytes[at..]);
        if n >= 0 { at += n; }
        else if n == -EAGAIN { guard ready_within(t.fd, POLLOUT, t.ms) else { return false; } }
        else { return false; }
    }
    return true;
}

fn pg::receive(t: pg::Socket&, into: u8[>..]&) -> bool {
    guard t.fd >= 0 else { return false; }
    // The facade passes into from the Db that holds t, which rules out
    // resizing it, so the bytes come through buf.
    var buf: u8[pg::RECEIVE_CHUNK] = [0; pg::RECEIVE_CHUNK];
    loop {
        let n = recv_some(t.fd, buf[..]);
        if n > 0 {
            into.append(buf[..n]);
            return true;
        }
        guard n == -EAGAIN else { return false; }
        guard ready_within(t.fd, POLLIN, t.ms) else { return false; }
    }
}

fn pg::close(t: pg::Socket&) {
    close_fd(t.fd);
    t.fd = -1;
}

// Connects db's socket to cfg's host and port, each wait limited to ms
// milliseconds (-1: none), then starts the session as connect does. A host
// that starts with / is a directory holding the server's Unix socket,
// .s.PGSQL.<port>, as in libpq. Closes db first when it is open, so a
// connection that broke is made again with open. A socket that can't connect
// fails with SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION.
fn open(db: Db<Socket>&, cfg: Config, ms) {
    close(db);
    let where = socket_address(cfg);
    let a, ok = address(where[..]);
    guard ok else { fail(SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, cant_connect(where[..], "the address is not numeric: no name is resolved")); }
    let fd, e = connect_start(a);
    guard e == 0 else { fail(SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, cant_connect(where[..], reason(-e))); }
    var result = 0;
    if ready_within(fd, POLLOUT, ms) { result = connect_result(fd); } else { result = -ETIMEDOUT; }
    guard result == 0 else {
        close_fd(fd);
        fail(SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, cant_connect(where[..], reason(-result)));
    }
    db.t = Socket { fd: fd, ms: ms };
    connect(db, cfg);
}

// What a Config's server is connected at: its host and port, or unix: and the
// server's socket in the directory a host that starts with / names. cfg is a
// Config; untyped, so that `goose check` of this file alone, which doesn't see
// pg.goose, takes it.
fn socket_address(cfg) -> u8[>..] {
    var where: u8[>..] = [];
    if cfg.host.len > 0 && cfg.host[0] == '/' {
        format(where, "unix:", cfg.host, if cfg.host[cfg.host.len - 1] == '/' { "" } else { "/" },
               ".s.PGSQL.", cfg.port);
    } else {
        format(where, cfg.host, ":", cfg.port);
    }
    return where;
}

// Why a connect to where failed, for an Error's message.
fn cant_connect(where: const u8[:], why: const u8[:]) -> u8[] {
    var message: u8[>..] = [];
    format(message, "can't connect to ", where, ": ", why);
    return str(message);
}

// Connects db with the settings in a postgres:// URL (parse_url), as open
// does with a Config.
fn open(db: Db<Socket>&, url: const u8[:], ms) {
    let cfg, why = url_settings(url);
    guard why.len == 0 else { fail(SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION, why); }
    open(db, cfg, ms);
}

// The common reasons a connect fails, by name; others by their number.
fn reason(code: i64) -> u8[] {
    var r: u8[>..] = [];
    if code == ECONNREFUSED { r.append("ECONNREFUSED"); }
    else if code == ETIMEDOUT { r.append("ETIMEDOUT"); }
    else if code == ENOENT { r.append("ENOENT"); }
    else if code == ENETUNREACH || code == EHOSTUNREACH { r.append("EHOSTUNREACH"); }
    else { format(r, "error ", code); }
    return str(r);
}
