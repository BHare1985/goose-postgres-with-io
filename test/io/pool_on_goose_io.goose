// The pool (pool.goose) on goose-io's in-memory provider, against a server scripted by time: a cleartext
// login, a query with a parameter and two rows, a second request that waits in the queue for the one
// connection and fails, a dial that is refused, a server at a Unix socket path, and a server that never answers. The first query is made
// before the loop runs, and the answers come after the session is ready.
import std;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import goose_io from "../../../..";
import goose_io.memory;
import pg from "../../";
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

struct App { t1: pg::Ticket = pg::Ticket { .. }, t2: pg::Ticket = pg::Ticket { .. }, finished: i64 = 0, ms: i64 = 1000, pool: pg::Pool }

fn sql_of(n: i64) -> const u8[:] { return if n == 1 { "SELECT id, name FROM items WHERE id < $1" } else { "SELEC $1" }; }

// One request's route: its query at its start and each time the pool names it, until it ends.
fn route(app: App&, t: pg::Ticket&, n: i64, io: goose_io_memory::Loop&) {
    guard t.state != pg::TICKET_DONE else { return; }
    let r = pg::query(app.pool, t, io, app.ms, sql_of(n), 3) { r => print("request ", n, " row: ", pg::int(r, 0), " ", pg::text(r, 1)); };
    if r == pg::WAITS { return; }
    if r == pg::FAILS { print("request ", n, " failed ", t.code[..], " ", app.pool.reason[..]); }
    else { print("request ", n, ": ", r, " rows"); }
    app.finished++;
}

fn event(app: App&, io: goose_io_memory::Loop&, ev: goose_io::Event) {
    pg::pool_event(app.pool, io, ev);
    loop {
        let key = pg::wake(app.pool);
        if key == 0 { break; }
        if key == app.t1.key { route(app, app.t1, 1, io); } else if key == app.t2.key { route(app, app.t2, 2, io); }
    }
    if app.finished == 2 { io.stop(0); }
}


// Another library on the same loop: it dials its own server and counts the bytes that come back.
struct Counter { link: goose_io::Link = goose_io::Link { .. }, bytes: i64 = 0, closed: bool = false }

// Returns true if the event was this library's, as a library on a shared loop must.
fn counter_event(c: Counter&, ev: goose_io::Event) -> bool {
    guard goose_io::is_link_event(ev) && goose_io::same(c.link, goose_io::link_of(ev)) else { return false; }
    match ev {
        Data e => { c.bytes += e.bytes.len; },
        Closed e => { c.closed = true; },
        _ => {},
    }
    return true;
}

struct Shared { counter: Counter, pool_took: i64 = 0, counter_took: i64 = 0, both: i64 = 0, app: App }

fn shared_event(sh: Shared&, io: goose_io_memory::Loop&, ev: goose_io::Event) {
    // Every library is offered every event; each takes only its own.
    let a = pg::pool_event(sh.app.pool, io, ev);
    let b = counter_event(sh.counter, ev);
    if a { sh.pool_took++; }
    if b { sh.counter_took++; }
    if a && b { sh.both++; }
    loop {
        let key = pg::wake(sh.app.pool);
        if key == 0 { break; }
        if key == sh.app.t1.key { route(sh.app, sh.app.t1, 1, io); }
    }
    if sh.app.finished == 2 && sh.counter.bytes == 11 { io.stop(0); }
}

fn config(host: const u8[:]) -> pg::Config {
    var cfg = pg::Config { .. };
    cfg.host.append(host);
    cfg.user.append("u");
    cfg.password.append("pw");
    cfg.database.append("db");
    cfg.allow_cleartext = true;
    return cfg;
}

fn main() {
    print("-- two requests, one connection: the second waits in the queue");
    var io = goose_io_memory::Loop { .. };
    let server = io.add_peer();
    io.peer_accept(server, "10.0.0.5:5432");
    io.peer_send(server, 1, check::message_of('R', check::auth(3, "")));
    io.peer_send(server, 5, login());
    io.peer_send(server, 20, first_answer());
    io.peer_send(server, 40, second_answer());
    var app = App { pool: pg::pool(config("10.0.0.5"), 1) };
    route(app, app.t1, 1, io);
    route(app, app.t2, 2, io);
    io.serve() { io, ev => event(app, io, ev); };
    let sent = io.peer_received(server);
    check::that("the password went in cleartext", find(sent[..], "p\0\0\0\x07pw\0") >= 0);
    check::that("the parameter went as text", find(sent[..], "\0\0\0\x013") >= 0);

    print("-- a server that refuses");
    var io2 = goose_io_memory::Loop { .. };
    io2.peer_refuse(io2.add_peer(), "10.0.0.6:5432");
    var app2 = App { finished: 1, pool: pg::pool(config("10.0.0.6"), 2) };
    app2.t2.state = pg::TICKET_DONE;
    route(app2, app2.t1, 1, io2);
    io2.serve() { io, ev => event(app2, io, ev); };

    print("-- a server on a Unix socket, as the host /var/run/postgresql names it");
    var io3 = goose_io_memory::Loop { .. };
    let unix_server = io3.add_peer();
    io3.peer_accept(unix_server, "unix:/var/run/postgresql/.s.PGSQL.5432");
    io3.peer_send(unix_server, 1, check::message_of('R', check::auth(3, "")));
    io3.peer_send(unix_server, 5, login());
    io3.peer_send(unix_server, 20, first_answer());
    var app3 = App { finished: 1, pool: pg::pool(config("/var/run/postgresql"), 1) };
    app3.t2.state = pg::TICKET_DONE;
    route(app3, app3.t1, 1, io3);
    io3.serve() { io, ev => event(app3, io, ev); };
    check::that("the query went to the path as it went to an address", find(io3.peer_received(unix_server)[..], "p\0\0\0\x07pw\0") >= 0);

    print("-- a server that never answers: the request that runs and the one that waits both time out");
    var io4 = goose_io_memory::Loop { .. };
    io4.peer_accept(io4.add_peer(), "10.0.0.7:5432");
    var app4 = App { ms: 300, pool: pg::pool(config("10.0.0.7"), 1) };
    route(app4, app4.t1, 1, io4);
    route(app4, app4.t2, 2, io4);
    io4.serve() { io, ev => event(app4, io, ev); };
    check::that("the timed-out request gave its connection up", app4.pool.slots[0].owner == 0);
    check::that("and the connection was hung up, as a server that doesn't answer may not answer at all", app4.pool.slots[0].phase == pg::SLOT_CLOSED);

    print("-- the pool and another library on one loop: each takes only its own events");
    var io5 = goose_io_memory::Loop { .. };
    let pg_server = io5.add_peer();
    io5.peer_accept(pg_server, "10.0.0.8:5432");
    io5.peer_send(pg_server, 1, check::message_of('R', check::auth(3, "")));
    io5.peer_send(pg_server, 5, login());
    io5.peer_send(pg_server, 20, first_answer());
    let other_server = io5.add_peer();
    io5.peer_accept(other_server, "10.0.0.9:7000");
    io5.peer_send(other_server, 10, "hello world");
    var sh = Shared { counter: Counter { .. }, app: App { finished: 1, pool: pg::pool(config("10.0.0.8"), 1) }, .. };
    sh.app.t2.state = pg::TICKET_DONE;
    sh.counter.link = io5.dial("10.0.0.9:7000", 1000);
    route(sh.app, sh.app.t1, 1, io5);
    io5.serve() { io, ev => shared_event(sh, io, ev); };
    check::that("the other library got its 11 bytes", sh.counter.bytes == 11);
    check::that("the pool's request finished", sh.app.finished == 2);
    check::that("no event was taken by both libraries", sh.both == 0);
    check::that("each library took events", sh.pool_took > 0 && sh.counter_took > 0);
}
