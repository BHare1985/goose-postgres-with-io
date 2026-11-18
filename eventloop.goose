// An event loop of the package's own: one thread, many connections, no
// runtime beneath it. It dials and listens, takes the bytes that arrive,
// holds what a socket won't take yet, and calls one handler with each event.
// The pool (pool.goose) runs on it, and a program may use it for its own
// sockets as well.
//
//   var lp = pg::Loop { .. };
//   let link = lp.dial("127.0.0.1:5432", 3000);       // before serve, or in the handler
//   lp.serve() { lp, ev =>
//       match ev {
//           Opened e => { lp.send(e.link, "hello"); },
//           Data e => { ... e.bytes, valid until the handler returns ... },
//           Closed e => { ... },
//           _ => {},
//       }
//   };
//
// A Link names one connection or listener: a slot and a generation, so a link
// that has ended names nothing even when its slot is used again. Every
// operation on such a link does nothing, and send says Closed.
//
// What the loop guarantees:
//   - A link that opens gets Opened first and Closed last, once each; a dial
//     that fails gets Closed and no Opened.
//   - An operation never blocks. send takes every byte or none: a send that
//     does not fit returns Full, and one Writable follows when it would.
//   - A handler may call any operation, on any link, from any event.
//   - serve returns when stop was called and every link has been closed.
//
// A loop belongs to one thread. Its descriptors are polled, which is as fast
// as the connections a database client keeps need.
namespace pg;

import std;
import .net;

// The most bytes of one connection's unsent output the loop holds, and the
// largest send it takes.
let LOOP_OUT = 65536;
// The bytes one read takes, and so the most one Data event carries.
let LOOP_READ = 65536;
// A Tick comes at least this often, in milliseconds.
let LOOP_TICK_MS = 100;
let LOOP_FOREVER = 0x7fffffffffffffff;

struct Link { slot: u32 = 0, gen: u32 = 0 }

fn is_valid(l: Link) -> bool { l.gen != 0 }
fn same(a: Link, b: Link) -> bool { a.slot == b.slot && a.gen == b.gen }

// Why a link ended.
enum LinkEnd {
    Peer,           // the peer closed, cleanly
    Reset,          // the peer or the network aborted it
    Refused,        // a dial: nothing accepts there
    TimedOut,       // a dial: no answer within its time
    Unreachable,    // a dial: no route
    BadAddress,     // a dial: the text is not an address, such as a host name, which nothing resolves
    Failed,         // anything else, and a dial that failed for another reason
    Local,          // this program closed it
    Stopped,        // the loop was stopped while it was open
}

enum SendResult {
    Sent,           // every byte taken; the caller may reuse its memory
    Full,           // not now; one Writable follows when it would fit
    TooLarge,       // more than max_send: it will never fit in one call
    NotOpen,        // a dial that has not opened yet
    Closed,         // ended, ending, or not a link of this loop
}

enum LoopEvent {
    // The link is open. listener is the listener that accepted it, invalid for a dialled link.
    Opened { link: Link, listener: Link },
    // Bytes from the peer, in order, valid only until the handler returns.
    Data { link: Link, bytes: const u8[:] },
    // A send that returned Full would now fit.
    Writable { link: Link },
    // The last event of a link.
    Closed { link: Link, why: LinkEnd },
    // At least every LOOP_TICK_MS, and at or after any time asked for with tick_by.
    Tick { now_ms: i64 },
}

// True for Opened, Data, Writable and Closed. A Tick belongs to everything on the loop.
fn is_link_event(ev: LoopEvent) -> bool {
    match ev {
        Tick t => false,
        _ => true,
    }
}

// The link of a link event, an invalid Link for a Tick.
fn link_of(ev: LoopEvent) -> Link {
    match ev {
        Opened e => e.link,
        Data e => e.link,
        Writable e => e.link,
        Closed e => e.link,
        _ => Link { .. },
    }
}

let LS_FREE: u8 = 0;
let LS_CONNECTING: u8 = 1;
let LS_OPEN: u8 = 2;
let LS_LISTENING: u8 = 3;

struct LoopSlot {
    gen: u32 = 1,
    state: u8 = LS_FREE,
    fd: i64 = -1,
    // Ended: the descriptor is closed, and Closed waits to be delivered.
    dead: bool = false,
    why: LinkEnd = LinkEnd.Failed,
    // A send returned Full since the last Writable.
    full: bool = false,
    // The listener that accepted it.
    from: Link = Link { .. },
    // A Unix listener's path, which the loop removes when the listener ends.
    path: u8[..108],
    deadline: i64 = LOOP_FOREVER,
    out_len: i64 = 0,
    out: u8[LOOP_OUT],
}

// The loop: build it empty, `pg::Loop { .. }`, and keep it last in whatever
// holds it, since its slots end it.
struct Loop {
    next_tick: i64 = 0,
    wake_at: i64 = LOOP_FOREVER,
    now: i64 = 0,
    stopping: bool = false,
    finished: bool = false,
    started: bool = false,
    scratch: u8[LOOP_READ],
    slots: LoopSlot[>..],
}

fn max_send(lp: Loop&) -> i64 { LOOP_OUT }

// The loop's clock, in milliseconds, which only moves forward.
fn now_ms(lp: Loop&) -> i64 {
    let t = clock_ms();
    if t > lp.now { lp.now = t; }
    return lp.now;
}

// Asks for a Tick at or after at_ms, on the clock of now_ms.
fn tick_by(lp: Loop&, at_ms: i64) { if at_ms < lp.wake_at { lp.wake_at = at_ms; } }

fn draining(lp: Loop&) -> bool { lp.stopping || lp.finished }

// The slot of l, or -1 when l names nothing now.
fn slot_of(lp: Loop&, l: Link) -> i64 {
    guard l.gen != 0 && l.slot < lp.slots.len else { return -1; }
    guard lp.slots[l.slot].gen == l.gen && lp.slots[l.slot].state != LS_FREE else { return -1; }
    return l.slot;
}

fn link_at(lp: Loop&, i: i64) -> Link { Link { slot: i as u32, gen: lp.slots[i].gen } }

fn new_slot(lp: Loop&) -> i64 {
    for i in lp.slots.len { if lp.slots[i].state == LS_FREE { return i; } }
    lp.slots.push(LoopSlot { .. });
    return lp.slots.len - 1;
}

// A slot that has ended: its descriptor is closed now, and its Closed goes out
// at the next pass. Does nothing to one that already ended.
fn end(lp: Loop&, i: i64, why: LinkEnd) {
    let s .= lp.slots[i];
    guard s.state != LS_FREE && !s.dead else { return; }
    close_fd(s.fd);
    if s.state == LS_LISTENING && s.path.len > 0 {
        unlink_path(s.path[..]);
        s.path.clear();
    }
    s.fd = -1;
    s.dead = true;
    s.why = why;
    s.out_len = 0;
}

// --- operations ----------------------------------------------------------------------------------

// Starts a connection to address (`a.b.c.d:port` or `unix:/path`) and returns
// its link. It opens when the connect completes, or ends within connect_ms
// milliseconds (0: no limit). An address that is not one, or a failure at
// once, gives a link that ends with Closed at the next pass, so a caller
// handles every failure in one place.
fn dial(lp: Loop&, where: const u8[:], connect_ms: i64) -> Link {
    let i = new_slot(lp);
    let s .= lp.slots[i];
    s.dead = false;
    s.full = false;
    s.out_len = 0;
    s.from = Link { .. };
    s.deadline = if connect_ms > 0 { lp.now_ms() + connect_ms } else { LOOP_FOREVER };
    let a, ok = address(where);
    s.state = LS_CONNECTING;
    guard ok else {
        s.why = LinkEnd.BadAddress;
        s.dead = true;
        return lp.link_at(i);
    }
    let fd, e = connect_start(a);
    if e != 0 {
        s.dead = true;
        s.why = ended_by(-e);
    } else {
        s.fd = fd;
    }
    return lp.link_at(i);
}

// How a failed connect, or a failed read, ended the link, from its errno.
fn ended_by(e: i64) -> LinkEnd {
    if e == ECONNREFUSED { return LinkEnd.Refused; }
    if e == ETIMEDOUT { return LinkEnd.TimedOut; }
    if e == ENETUNREACH || e == EHOSTUNREACH { return LinkEnd.Unreachable; }
    if e == ECONNRESET || e == EPIPE { return LinkEnd.Reset; }
    return LinkEnd.Failed;
}

// Listens on address; each connection it accepts opens with Opened naming the
// returned link. The errno on failure, 0 for none.
fn listen(lp: Loop&, where: const u8[:]) -> Link, i64 {
    let a, ok = address(where);
    guard ok else { return Link { .. }, EACCES; }
    let fd = listen_on(a);
    guard fd >= 0 else { return Link { .. }, -fd; }
    let i = new_slot(lp);
    let s .= lp.slots[i];
    s.state = LS_LISTENING;
    s.fd = fd;
    s.path.clear();
    if is_unix(a) { s.path.append(where[5..]); }
    s.dead = false;
    s.full = false;
    s.out_len = 0;
    s.deadline = LOOP_FOREVER;
    return lp.link_at(i), 0;
}

// The port a TCP listener is bound to, for one made with port 0; 0 otherwise.
fn listener_port(lp: Loop&, l: Link) -> i64 {
    let i = lp.slot_of(l);
    guard i >= 0 && lp.slots[i].state == LS_LISTENING && !lp.slots[i].dead else { return 0; }
    return bound_port(lp.slots[i].fd);
}

// Takes every byte of bytes, or none. A send that comes back Full is
// followed by one Writable.
fn send(lp: Loop&, l: Link, bytes: const u8[:]) -> SendResult {
    let i = lp.slot_of(l);
    guard i >= 0 else { return SendResult.Closed; }
    let s .= lp.slots[i];
    guard !s.dead && s.state != LS_LISTENING else { return SendResult.Closed; }
    guard s.state == LS_OPEN else { return SendResult.NotOpen; }
    guard bytes.len <= LOOP_OUT else { return SendResult.TooLarge; }
    guard bytes.len > 0 else { return SendResult.Sent; }
    if s.out_len > 0 {
        guard s.out_len + bytes.len <= LOOP_OUT else {
            s.full = true;
            return SendResult.Full;
        }
        for k in bytes.len { s.out[s.out_len + k] = bytes[k]; }
        s.out_len += bytes.len;
        return SendResult.Sent;
    }
    var n = send_some(s.fd, bytes);
    if n == -EAGAIN { n = 0; }
    guard n >= 0 else {
        lp.end(i, ended_by(-n));
        return SendResult.Closed;
    }
    for k in bytes.len - n { s.out[k] = bytes[n + k]; }
    s.out_len = bytes.len - n;
    return SendResult.Sent;
}

// Ends the link now. What it had not sent is dropped. Its Closed comes at the
// next pass.
fn close(lp: Loop&, l: Link) {
    let i = lp.slot_of(l);
    guard i >= 0 else { return; }
    lp.end(i, LinkEnd.Local);
}

// Ends the loop: every link is closed, and serve returns once their Closed
// events are delivered.
fn stop(lp: Loop&) { lp.stopping = true; }

// --- serve ---------------------------------------------------------------------------------------

// Runs the loop, calling F with each event, until stop was called and the
// links have all ended. A second call after serve returned returns at once.
fn serve<F>(lp: Loop&) {
    loop { if !lp.step(-1, F) { break; } }
}

// One pass of the loop, for a program that owns a loop of its own and runs this one beside it: it
// delivers what has ended and what is due, waits for readiness for no longer than max_wait_ms (-1: as
// long as the next timer needs, 0: not at all), delivers what is ready, and returns. False once the
// loop has finished, after stop. serve is this in a loop. The wait is a poll over the loop's own
// descriptors, which a host cannot add to its own poll set, so a host that blocks elsewhere calls
// step at the rate it needs the loop's timers and reads served, with a small max_wait_ms.
fn step<F>(lp: Loop&, max_wait_ms: i64) -> bool {
    guard !lp.finished else { return false; }
    if !lp.started {
        lp.started = true;
        lp.now_ms();
        lp.next_tick = lp.now + LOOP_TICK_MS;
    }
    if lp.stopping {
        for i in lp.slots.len {
            if lp.slots[i].state != LS_FREE { lp.end(i, LinkEnd.Stopped); }
        }
    }
    // The links that ended, and the dials that ran out of time.
    lp.now_ms();
    for i in lp.slots.len {
        let s .= lp.slots[i];
        if s.state == LS_CONNECTING && !s.dead && lp.now >= s.deadline { lp.end(i, LinkEnd.TimedOut); }
    }
    var closed = 0;
    for i in lp.slots.len {
        let s .= lp.slots[i];
        if s.state != LS_FREE && s.dead {
            let l = lp.link_at(i);
            let e: LoopEvent = LoopEvent.Closed { link: l, why: s.why };
            // The slot is free, and its generation moves on, before the handler runs, so a
            // dial the handler makes may reuse it and the old link names nothing.
            s.state = LS_FREE;
            s.dead = false;
            s.gen = if s.gen == 0xffffffff { 1 } else { s.gen + 1 };
            F(lp, e);
            closed++;
        }
    }
    if lp.stopping {
        var live = false;
        for i in lp.slots.len { if lp.slots[i].state != LS_FREE { live = true; } }
        if !live {
            lp.finished = true;
            return false;
        }
        return true;
    }
    if lp.now >= lp.next_tick || lp.now >= lp.wake_at {
        lp.next_tick = lp.now + LOOP_TICK_MS;
        lp.wake_at = LOOP_FOREVER;
        let e: LoopEvent = LoopEvent.Tick { now_ms: lp.now };
        F(lp, e);
    }
    lp.poll_once(closed > 0, max_wait_ms, F);
    return true;
}

// Waits for readiness, for no longer than the next thing that is due, and
// handles what is ready. immediate: do not wait, since a handler already
// has work (it closed a link, say) to be seen at the next pass.
fn poll_once<F>(lp: Loop&, immediate: bool, max_wait_ms: i64) {
    var fds: PollFd[>..] = [];
    var at: i32[>..] = [];
    var due = lp.next_tick;
    if lp.wake_at < due { due = lp.wake_at; }
    var any_dead = false;
    for i in lp.slots.len {
        let s .= lp.slots[i];
        guard s.state != LS_FREE else { continue; }
        if s.dead { any_dead = true; continue; }
        if s.deadline < due { due = s.deadline; }
        var events: i16 = 0;
        if s.state == LS_CONNECTING { events = POLLOUT; }
        else {
            events = POLLIN;
            if s.out_len > 0 { events = events | POLLOUT; }
        }
        fds.push(PollFd { fd: s.fd as i32, events: events, .. });
        at.push(i as i32);
    }
    var wait = due - lp.now_ms();
    if max_wait_ms >= 0 && wait > max_wait_ms { wait = max_wait_ms; }
    if wait < 0 || immediate || any_dead { wait = 0; }
    let n = wait_ready(fds[..], wait);
    guard n > 0 else { return; }
    for k in fds.len {
        guard fds[k].revents != 0 else { continue; }
        let i = at[k] as i64;
        let g = lp.slots[i].gen;
        // A handler that ran earlier in this pass may have ended or replaced this slot.
        guard lp.slots[i].gen == g && lp.slots[i].state != LS_FREE && !lp.slots[i].dead else { continue; }
        lp.ready(i, fds[k].revents, F);
    }
}

fn ready<F>(lp: Loop&, i: i64, revents: i16) {
    let s .= lp.slots[i];
    if s.state == LS_LISTENING {
        loop {
            let c = accept_one(s.fd);
            guard c >= 0 else { break; }
            let j = lp.new_slot();
            let t .= lp.slots[j];
            t.state = LS_OPEN;
            t.fd = c;
            t.dead = false;
            t.full = false;
            t.out_len = 0;
            t.deadline = LOOP_FOREVER;
            t.from = lp.link_at(i);
            let e: LoopEvent = LoopEvent.Opened { link: lp.link_at(j), listener: lp.link_at(i) };
            F(lp, e);
            // The handler may have grown the slots, so look at the listener again.
            guard lp.slots[i].state == LS_LISTENING && !lp.slots[i].dead else { return; }
        }
        return;
    }
    if s.state == LS_CONNECTING {
        guard (revents & (POLLOUT | POLLERR | POLLHUP)) != 0 else { return; }
        let r = connect_result(s.fd);
        guard r == 0 else {
            lp.end(i, ended_by(-r));
            return;
        }
        s.state = LS_OPEN;
        s.deadline = LOOP_FOREVER;
        let e: LoopEvent = LoopEvent.Opened { link: lp.link_at(i), listener: Link { .. } };
        F(lp, e);
        return;
    }
    if (revents & POLLOUT) != 0 && s.out_len > 0 { lp.flush(i, F); }
    guard lp.slots[i].state == LS_OPEN && !lp.slots[i].dead else { return; }
    if (revents & (POLLIN | POLLERR | POLLHUP)) != 0 {
        let n = recv_some(s.fd, lp.scratch[..]);
        if n > 0 {
            let e: LoopEvent = LoopEvent.Data { link: lp.link_at(i), bytes: lp.scratch[..n] };
            F(lp, e);
        } else if n == 0 {
            lp.end(i, LinkEnd.Peer);
        } else if n != -EAGAIN {
            lp.end(i, ended_by(-n));
        }
    }
}

// Sends what a link held back, and says Writable once it has taken it all
// when a send had been refused.
fn flush<F>(lp: Loop&, i: i64) {
    let s .= lp.slots[i];
    var n = send_some(s.fd, s.out[..s.out_len]);
    if n == -EAGAIN { n = 0; }
    guard n >= 0 else {
        lp.end(i, ended_by(-n));
        return;
    }
    let rest = s.out_len - n;
    for k in rest { s.out[k] = s.out[n + k]; }
    s.out_len = rest;
    if s.full && s.out_len < LOOP_OUT {
        s.full = false;
        let e: LoopEvent = LoopEvent.Writable { link: lp.link_at(i) };
        F(lp, e);
    }
}
