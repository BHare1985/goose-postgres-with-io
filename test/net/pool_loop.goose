// The pool (pool.goose) on the package's own event loop, over real sockets: a
// server scripted in Goose runs on the same loop and speaks the protocol. A
// cleartext login, a query with a parameter and two rows, a second request that
// waits in the queue for the one connection and fails with the server's error, a
// dial that is refused, a host name, a server that never answers, and another
// library on the same loop that takes only its own events.
import std;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "../../";
import pg.eventloop;
import pg.looppool;
import pg.pool;
import check from "../lib/check/";

// RowDescription for an int4 column "id" and a text column "name".
fn description() -> u8[>..] {
    var b: u8[>..] = [];
    pg::put16(b, 2);
    b.append("id\0");
    pg::put32(b, 0);
    pg::put16(b, 0);
    pg::put32(b, pg::OID_INT4);
    pg::put16(b, 4);
    pg::put32(b, -1);
    pg::put16(b, 0);
    b.append("name\0");
    pg::put32(b, 0);
    pg::put16(b, 0);
    pg::put32(b, pg::OID_TEXT);
    pg::put16(b, -1);
    pg::put32(b, -1);
    pg::put16(b, 0);
    var out: u8[>..] = [];
    check::message(out, 'T', b);
    return out;
}

fn data_row(id: const u8[:], name: const u8[:]) -> u8[>..] {
    var b: u8[>..] = [];
    pg::put16(b, 2);
    pg::put32(b, id.len);
    b.append(id);
    pg::put32(b, name.len);
    b.append(name);
    var out: u8[>..] = [];
    check::message(out, 'D', b);
    return out;
}

fn login() -> u8[>..] {
    var out: u8[>..] = [];
    check::message(out, 'R', check::auth(0, ""));
    check::message(out, 'S', "client_encoding\0UTF8\0");
    check::message(out, 'K', "\0\0\x01\x02\0\0\0\x07");
    check::message(out, 'Z', "I");
    return out;
}

fn first_answer() -> u8[>..] {
    var out: u8[>..] = [];
    check::message(out, '1', "");
    check::message(out, '2', "");
    out.append(description());
    out.append(data_row("1", "one"));
    out.append(data_row("2", "two"));
    check::message(out, 'C', "SELECT 2\0");
    check::message(out, 'Z', "I");
    return out;
}

fn second_answer() -> u8[>..] {
    var out: u8[>..] = [];
    check::message(out, 'E', "SERROR\0C42601\0Msyntax error\0\0");
    check::message(out, 'Z', "I");
    return out;
}

// The server: it accepts on its listener and answers each flight the client
// sends. A flight is what arrives in one Data event, which on a loopback
// socket is one write of the client's. With silent set it accepts and says nothing.
struct Server {
    listener: pg::Link = pg::Link { .. },
    conn: pg::Link = pg::Link { .. },
    silent: bool = false,
    flights: i64 = 0,
    received: i64 = 0,
    saw_password: bool = false,
    saw_parameter: bool = false,
}

fn server_event(sv: Server&, lp: pg::Loop&, ev: pg::LoopEvent) {
    match ev {
        Opened e => {
            if pg::is_valid(e.listener) && pg::same(e.listener, sv.listener) { sv.conn = e.link; }
        },
        Data e => {
            guard pg::same(e.link, sv.conn) else { return; }
            sv.received += e.bytes.len;
            if find(e.bytes, "p\0\0\0\x07pw\0") >= 0 { sv.saw_password = true; }
            if find(e.bytes, "\0\0\0\x013") >= 0 { sv.saw_parameter = true; }
            guard !sv.silent else { return; }
            sv.flights++;
            if sv.flights == 1 { lp.send(e.link, check::message_of('R', check::auth(3, ""))[..]); }
            else if sv.flights == 2 { lp.send(e.link, login()[..]); }
            else if sv.flights == 3 { lp.send(e.link, first_answer()[..]); }
            else if sv.flights == 4 { lp.send(e.link, second_answer()[..]); }
        },
        _ => {},
    }
}

// Another library on the same loop: it dials its own server and counts the bytes that come back.
struct Counter { link: pg::Link = pg::Link { .. }, bytes: i64 = 0, closed: bool = false }

// Returns true if the event was this library's, as a library on a shared loop must.
fn counter_event(c: Counter&, ev: pg::LoopEvent) -> bool {
    guard pg::is_link_event(ev) && pg::same(c.link, pg::link_of(ev)) else { return false; }
    match ev {
        Data e => { c.bytes += e.bytes.len; },
        Closed e => { c.closed = true; },
        _ => {},
    }
    return true;
}

struct App {
    t1: pg::Ticket = pg::Ticket { .. },
    t2: pg::Ticket = pg::Ticket { .. },
    finished: i64 = 0,
    ms: i64 = 2000,
    // Two requests that time out together end in either order on a real clock, so a scenario
    // that has them reports their outcomes afterwards, in a fixed order.
    quiet: bool = false,
    pool: pg::Pool,
}

struct World {
    sv: Server = Server { .. },
    other: Server = Server { .. },
    counter: Counter = Counter { .. },
    pool_took: i64 = 0,
    counter_took: i64 = 0,
    both: i64 = 0,
    wanted: i64 = 2,
    app: App,
}

fn sql_of(n: i64) -> const u8[:] { return if n == 1 { "SELECT id, name FROM items WHERE id < $1" } else { "SELEC $1" }; }

// One request's route: its query at its start and each time the pool names it, until it ends.
fn route(app: App&, t: pg::Ticket&, n: i64, lp: pg::Loop&) {
    guard t.state != pg::TICKET_DONE else { return; }
    let r = pg::query(app.pool, t, lp, app.ms, sql_of(n), 3) { r => print("request ", n, " row: ", pg::int(r, 0), " ", pg::text(r, 1)); };
    if r == pg::WAITS { return; }
    if !app.quiet {
        if r == pg::FAILS { print("request ", n, " failed ", t.code[..], " ", app.pool.reason[..]); }
        else { print("request ", n, ": ", r, " rows"); }
    }
    app.finished++;
}

fn event(w: World&, lp: pg::Loop&, ev: pg::LoopEvent) {
    server_event(w.sv, lp, ev);
    if pg::is_valid(w.other.listener) {
        // The other library's server sends its greeting when it accepts.
        if pg::is_valid(pg::link_of(ev)) {
            match ev {
                Opened e => {
                    if pg::is_valid(e.listener) && pg::same(e.listener, w.other.listener) { lp.send(e.link, "hello world"); }
                },
                _ => {},
            }
        }
    }
    // Every library is offered every event; each takes only its own.
    let a = pg::pool_event(w.app.pool, lp, ev);
    let b = counter_event(w.counter, ev);
    if a { w.pool_took++; }
    if b { w.counter_took++; }
    if a && b { w.both++; }
    loop {
        let key = pg::wake(w.app.pool);
        if key == 0 { break; }
        if key == w.app.t1.key { route(w.app, w.app.t1, 1, lp); } else if key == w.app.t2.key { route(w.app, w.app.t2, 2, lp); }
    }
    let done = w.app.finished == w.wanted && (!pg::is_valid(w.counter.link) || w.counter.bytes == 11);
    match ev {
        Tick t => { if done { lp.stop(); } },
        _ => { if done { lp.stop(); } },
    }
}

fn config(host: const u8[:], port: i64) -> pg::Config {
    var cfg = pg::Config { .. };
    cfg.host.append(host);
    cfg.port = port;
    cfg.user.append("u");
    cfg.password.append("pw");
    cfg.database.append("db");
    cfg.allow_cleartext = true;
    return cfg;
}

fn main() {
    print("-- two requests, one connection: the second waits in the queue");
    var lp = pg::Loop { .. };
    var w = World { app: App { pool: pg::pool(config("127.0.0.1", 0), 1) } };
    let l, e = lp.listen("127.0.0.1:0");
    w.sv.listener = l;
    w.app.pool.cfg.port = lp.listener_port(l);
    route(w.app, w.app.t1, 1, lp);
    route(w.app, w.app.t2, 2, lp);
    lp.serve() { lp, ev => event(w, lp, ev); };
    check::that("the password went in cleartext", w.sv.saw_password);
    check::that("the parameter went as text", w.sv.saw_parameter);

    print("-- a server that refuses");
    var lp2 = pg::Loop { .. };
    var w2 = World { app: App { finished: 1, pool: pg::pool(config("127.0.0.1", 1), 2) } };
    w2.app.t2.state = pg::TICKET_DONE;
    route(w2.app, w2.app.t1, 1, lp2);
    lp2.serve() { lp, ev => event(w2, lp, ev); };

    print("-- a host name: the loop resolves none");
    var lp3 = pg::Loop { .. };
    var w3 = World { app: App { finished: 1, pool: pg::pool(config("db.example.com", 5432), 1) } };
    w3.app.t2.state = pg::TICKET_DONE;
    route(w3.app, w3.app.t1, 1, lp3);
    lp3.serve() { lp, ev => event(w3, lp, ev); };

    print("-- a server that never answers: the request that runs and the one that waits both time out");
    var lp4 = pg::Loop { .. };
    var w4 = World { app: App { ms: 300, quiet: true, pool: pg::pool(config("127.0.0.1", 0), 1) } };
    w4.sv.silent = true;
    let l4, e4 = lp4.listen("127.0.0.1:0");
    w4.sv.listener = l4;
    w4.app.pool.cfg.port = lp4.listener_port(l4);
    route(w4.app, w4.app.t1, 1, lp4);
    route(w4.app, w4.app.t2, 2, lp4);
    lp4.serve() { lp, ev => event(w4, lp, ev); };
    print("request 1 failed ", w4.app.t1.code[..], " ", w4.app.pool.reason[..]);
    print("request 2 failed ", w4.app.t2.code[..]);
    check::that("the timed-out request gave its connection up", w4.app.pool.slots[0].owner == 0);
    check::that("and the connection was hung up, as a server that doesn't answer may not answer at all", w4.app.pool.slots[0].phase == pg::SLOT_CLOSED);

    print("-- the pool and another library on one loop: each takes only its own events");
    var lp5 = pg::Loop { .. };
    var w5 = World { app: App { finished: 1, pool: pg::pool(config("127.0.0.1", 0), 1) } };
    w5.app.t2.state = pg::TICKET_DONE;
    let l5, e5 = lp5.listen("127.0.0.1:0");
    w5.sv.listener = l5;
    w5.app.pool.cfg.port = lp5.listener_port(l5);
    let ol, oe = lp5.listen("127.0.0.1:0");
    w5.other.listener = ol;
    var addr: u8[>..] = [];
    format(addr, "127.0.0.1:", lp5.listener_port(ol));
    w5.counter.link = lp5.dial(addr[..], 2000);
    route(w5.app, w5.app.t1, 1, lp5);
    lp5.serve() { lp, ev => event(w5, lp, ev); };
    check::that("the other library got its 11 bytes", w5.counter.bytes == 11);
    check::that("the pool's request finished", w5.app.finished == 2);
    check::that("no event was taken by both libraries", w5.both == 0);
    check::that("each library took events", w5.pool_took > 0 && w5.counter_took > 0);
}
