// Sockets and readiness, bound from the C library, for the package's own event
// loop (loop.goose) and blocking socket (socket.goose). Nothing here needs an
// I/O library: the functions are bound by name when the program starts,
// without C code of the package's own, and the program links nothing extra.
//
// Linux only for now: the constants and the socket address layout below are
// Linux's (docs/development.md, "Platforms").
//
// Every function takes and returns plain values. A call that fails returns
// the negative of its errno, so a caller tells "would block" from "failed" by
// comparing with EAGAIN and the others below, and never reads errno itself.
namespace pg;

import std;
import ffi;

let AF_INET: i32 = 2;
let AF_UNIX: i32 = 1;
let SOCK_STREAM: i32 = 1;
let SOCK_NONBLOCK: i32 = 0x800;
let SOCK_CLOEXEC: i32 = 0x80000;
let MSG_NOSIGNAL: i32 = 0x4000;
let SOL_SOCKET: i32 = 1;
let SO_ERROR: i32 = 4;
let SO_REUSEADDR: i32 = 2;
let IPPROTO_TCP: i32 = 6;
let TCP_NODELAY: i32 = 1;
let SHUT_WR: i32 = 1;
let CLOCK_MONOTONIC: i32 = 1;

let EPERM = 1;
let ENOENT = 2;
let EINTR = 4;
let EACCES = 13;
let EMFILE = 24;
let EPIPE = 32;
let EAGAIN = 11;
let EADDRINUSE = 98;
let ENETUNREACH = 101;
let ECONNRESET = 104;
let ETIMEDOUT = 110;
let ECONNREFUSED = 111;
let EHOSTUNREACH = 113;
let EINPROGRESS = 115;

// poll's event bits.
let POLLIN: i16 = 1;
let POLLOUT: i16 = 4;
let POLLERR: i16 = 8;
let POLLHUP: i16 = 16;
let POLLNVAL: i16 = 32;

// One entry of poll's table: the descriptor, what to wait for, and what
// happened. The layout is C's struct pollfd.
struct PollFd { fd: i32 = -1, events: i16 = 0, revents: i16 = 0 }

// A socket address as the system takes it: sockaddr_in or sockaddr_un in the
// bytes, with the length to pass.
struct SockAddr { b: u8[110] = [0; 110], len: u32 = 0 }

// A C slice crosses as its pointer and its length, in the two registers a C
// function takes a buffer and a size in, so `send(fd, bytes, flags)` below is
// C's send(fd, buf, len, flags). This holds on the 64-bit Linux ABIs.
extern "libc.so.6" {
    fn c_socket(domain: i32, kind: i32, protocol: i32) -> i32 = "socket";
    fn c_connect(fd: i32, addr: const SockAddr&, len: u32) -> i32 = "connect";
    fn c_bind(fd: i32, addr: const SockAddr&, len: u32) -> i32 = "bind";
    fn c_listen(fd: i32, backlog: i32) -> i32 = "listen";
    fn c_accept4(fd: i32, addr: u64, len: u64, flags: i32) -> i32 = "accept4";
    fn c_send(fd: i32, data: const u8[:], flags: i32) -> i64 = "send";
    fn c_recv(fd: i32, data: u8[:], flags: i32) -> i64 = "recv";
    fn c_poll(fds: PollFd[:], timeout_ms: i32) -> i32 = "poll";
    fn c_close(fd: i32) -> i32 = "close";
    fn c_shutdown(fd: i32, how: i32) -> i32 = "shutdown";
    fn c_getsockopt(fd: i32, level: i32, name: i32, value: i32&, len: u32&) -> i32 = "getsockopt";
    fn c_setsockopt(fd: i32, level: i32, name: i32, value: const i32&, len: u32) -> i32 = "setsockopt";
    fn c_getsockname(fd: i32, addr: SockAddr&, len: u32&) -> i32 = "getsockname";
    fn c_clock_gettime(clock: i32, out: i64[2]&) -> i32 = "clock_gettime";
    fn c_errno_location() -> u64 = "__errno_location";
    fn c_unlink(path: const u8[:]) -> i32 = "unlink";
}

// The errno of the call that just failed on this thread.
fn errno() -> i64 {
    let p = ffi::bytes(c_errno_location(), 4);
    return p[0] | ((p[1] as i64) << 8) | ((p[2] as i64) << 16) | ((p[3] as i64) << 24);
}

// The monotonic clock, in milliseconds.
fn clock_ms() -> i64 {
    var t: i64[2] = [0; 2];
    c_clock_gettime(CLOCK_MONOTONIC, t);
    return t[0] * 1000 + t[1] / 1000000;
}

// --- addresses -----------------------------------------------------------------------------------

fn digit(c: u8) -> i64 { if c >= '0' && c <= '9' { c - '0' } else { -1 } }

// An address as the package takes it: `a.b.c.d:port` (and `localhost:port`),
// or `unix:/path` for a Unix socket. No name is resolved: a name is a
// failure, "the address isn't numeric", because resolving blocks, and a loop
// that blocks is not a loop (docs/decisions.md). ok is false for text that
// is not an address.
fn address(text: const u8[:]) -> SockAddr, bool {
    var a = SockAddr { .. };
    if starts_with(text, "unix:") {
        let path = text[5..];
        guard path.len > 0 && path.len < 108 else { return a, false; }
        a.b[0] = AF_UNIX as u8;
        for i in path.len { a.b[2 + i] = path[i]; }
        a.len = (2 + path.len + 1) as u32;
        return a, true;
    }
    var colon = -1;
    for i in text.len { if text[i] == ':' { colon = i; } }
    guard colon > 0 && colon + 1 < text.len else { return a, false; }
    var port = 0;
    for i in colon + 1..text.len {
        let d = digit(text[i]);
        guard d >= 0 else { return a, false; }
        port = port * 10 + d;
        guard port <= 65535 else { return a, false; }
    }
    let found = if text[..colon] == "localhost" { dotted_quad("127.0.0.1", a) } else { dotted_quad(text[..colon], a) };
    guard found else { return a, false; }
    a.b[0] = AF_INET as u8;
    a.b[2] = (port >> 8) as u8;
    a.b[3] = port as u8;
    a.len = 16;
    return a, true;
}

// Writes the four octets of host, a.b.c.d, into a's address bytes.
fn dotted_quad(host: const u8[:], a: SockAddr&) -> bool {
    var octet = 0;
    var digits = 0;
    var n = 0;
    for i in host.len + 1 {
        if i == host.len || host[i] == '.' {
            guard digits > 0 && octet <= 255 && n < 4 else { return false; }
            a.b[4 + n] = octet as u8;
            n++;
            octet = 0;
            digits = 0;
        } else {
            let d = digit(host[i]);
            guard d >= 0 else { return false; }
            octet = octet * 10 + d;
            digits++;
        }
    }
    return n == 4;
}

fn is_unix(a: SockAddr) -> bool { a.b[0] == AF_UNIX as u8 }

// --- sockets -------------------------------------------------------------------------------------

// A new non-blocking, close-on-exec stream socket for a, or the negative errno.
fn open_socket(a: SockAddr) -> i64 {
    let fd = c_socket(if is_unix(a) { AF_UNIX } else { AF_INET }, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    guard fd >= 0 else { return -errno(); }
    if !is_unix(a) {
        var one: i32 = 1;
        c_setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, one, 4);
    }
    return fd;
}

// Starts a connect on a new socket: the descriptor, and 0 when it is
// connecting or connected (wait for POLLOUT, then connect_result), or the
// negative errno when it failed at once.
fn connect_start(a: SockAddr) -> i64, i64 {
    let fd = open_socket(a);
    guard fd >= 0 else { return -1, fd; }
    guard c_connect(fd as i32, a, a.len) != 0 else { return fd, 0; }
    let e = errno();
    if e == EINPROGRESS || e == EINTR { return fd, 0; }
    c_close(fd as i32);
    return -1, -e;
}

// How a connect that polled writable ended: 0, or the negative errno.
fn connect_result(fd: i64) -> i64 {
    var err: i32 = 0;
    var len: u32 = 4;
    guard c_getsockopt(fd as i32, SOL_SOCKET, SO_ERROR, err, len) == 0 else { return -errno(); }
    return -(err as i64);
}

// A listening socket on a, and the negative errno when it failed. A TCP
// socket may reuse its address at once.
fn listen_on(a: SockAddr) -> i64 {
    let fd = open_socket(a);
    guard fd >= 0 else { return fd; }
    if !is_unix(a) {
        var one: i32 = 1;
        c_setsockopt(fd as i32, SOL_SOCKET, SO_REUSEADDR, one, 4);
    }
    if c_bind(fd as i32, a, a.len) != 0 || c_listen(fd as i32, 128) != 0 {
        let e = errno();
        c_close(fd as i32);
        return -e;
    }
    return fd;
}

// The port a listening TCP socket is bound to (0 for a Unix socket).
fn bound_port(fd: i64) -> i64 {
    var a = SockAddr { .. };
    var len: u32 = 110;
    guard c_getsockname(fd as i32, a, len) == 0 && a.b[0] == AF_INET as u8 else { return 0; }
    return ((a.b[2] as i64) << 8) | a.b[3];
}

// The next connection waiting at a listening socket, non-blocking, or the
// negative errno (EAGAIN: none).
fn accept_one(fd: i64) -> i64 {
    let c = c_accept4(fd as i32, 0, 0, SOCK_NONBLOCK | SOCK_CLOEXEC);
    guard c >= 0 else { return -errno(); }
    var one: i32 = 1;
    c_setsockopt(c, IPPROTO_TCP, TCP_NODELAY, one, 4);
    return c;
}

// Sends what the socket takes now: the count, possibly less than all, or the
// negative errno (EAGAIN: none fits now).
fn send_some(fd: i64, bytes: const u8[:]) -> i64 {
    let n = c_send(fd as i32, bytes, MSG_NOSIGNAL);
    guard n >= 0 else { return -errno(); }
    return n;
}

// Receives what has arrived into buf: the count, 0 when the peer closed, or
// the negative errno (EAGAIN: nothing yet).
fn recv_some(fd: i64, buf: u8[:]) -> i64 {
    let n = c_recv(fd as i32, buf, 0);
    guard n >= 0 else { return -errno(); }
    return n;
}

fn shutdown_write(fd: i64) { c_shutdown(fd as i32, SHUT_WR); }
fn close_fd(fd: i64) { if fd >= 0 { c_close(fd as i32); } }

// Removes the file at a Unix socket path, which only the program that bound it
// does. c_unlink takes the path as a C string, so it is passed with its NUL
// (the slice's pointer is all the C function reads).
fn unlink_path(path: const u8[:]) {
    var z: u8[>..] = [];
    z.append(path);
    z.push(0);
    c_unlink(z[..]);
}

// Waits until a descriptor of fds is ready or ms milliseconds pass (-1: no
// limit), and returns the count ready, or the negative errno. An interrupted
// wait counts as none ready.
fn wait_ready(fds: PollFd[:], ms: i64) -> i64 {
    let n = c_poll(fds, ms as i32);
    if n >= 0 { return n; }
    let e = errno();
    return if e == EINTR { 0 } else { -e };
}
