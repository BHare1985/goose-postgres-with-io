// The pool (pool.goose) on a host that is not the package's event loop: a toy type that
// knows nothing of pg.eventloop, declares the four operations the pool asks of its host,
// and calls the four event functions, as a server with a loop of its own would. A scripted
// Postgres answers each flight the pool sends. This program imports pg.pool and no loop.
import std;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "../../";
import pg.pool;
import check from "../lib/check/";

// The host: handles are small integers it hands out, and what the pool sent is kept for
// the scripted server to answer.
struct Host {
    next: i64 = 1,
    now: i64 = 0,
    dialled: i64 = 0,
    refuse: bool = false,
    open: i64 = 0,
    flights: i64 = 0,
    saw_password: bool = false,
    sent: u8[>..],
}

fn pg::host_dial(h: Host&, where: const u8[:], ms: i64) -> i64 {
    if h.refuse { return 0; }
    h.dialled++;
    h.open = h.next;
    h.next++;
    print("host: dial ", where, " within ", ms, " ms");
    return h.open;
}

fn pg::host_send(h: Host&, handle: i64, bytes: const u8[:]) -> bool {
    guard handle == h.open else { return false; }
    h.sent.clear();
    h.sent.append(bytes);
    h.flights++;
    if find(bytes, "p\0\0\0\x07pw\0") >= 0 { h.saw_password = true; }
    return true;
}

fn pg::host_close(h: Host&, handle: i64) { if handle == h.open { h.open = 0; } }

fn pg::host_now_ms(h: Host&) -> i64 { h.now }

fn description() -> u8[>..] {
    var b: u8[>..] = [];
    pg::put16(b, 1);
    b.append("id\0");
    pg::put32(b, 0);
    pg::put16(b, 0);
    pg::put32(b, pg::OID_INT4);
    pg::put16(b, 4);
    pg::put32(b, -1);
    pg::put16(b, 0);
    var out: u8[>..] = [];
    check::message(out, 'T', b);
    return out;
}

fn row(v: const u8[:]) -> u8[>..] {
    var b: u8[>..] = [];
    pg::put16(b, 1);
    pg::put32(b, v.len);
    b.append(v);
    var out: u8[>..] = [];
    check::message(out, 'D', b);
    return out;
}

// What the scripted server answers to the pool's nth flight.
fn answer(flight: i64) -> u8[>..] {
    var out: u8[>..] = [];
    if flight == 1 { check::message(out, 'R', check::auth(3, "")); }
    else if flight == 2 {
        check::message(out, 'R', check::auth(0, ""));
        check::message(out, 'S', "client_encoding\0UTF8\0");
        check::message(out, 'K', "\0\0\x01\x02\0\0\0\x07");
        check::message(out, 'Z', "I");
    } else {
        check::message(out, '1', "");
        check::message(out, '2', "");
        out.append(description());
        out.append(row("41"));
        out.append(row("42"));
        check::message(out, 'C', "SELECT 2\0");
        check::message(out, 'Z', "I");
    }
    return out;
}

fn config(host: const u8[:]) -> pg::Config {
    var cfg = pg::Config { .. };
    cfg.host.append(host);
    cfg.port = 5432;
    cfg.user.append("u");
    cfg.password.append("pw");
    cfg.database.append("db");
    cfg.allow_cleartext = true;
    return cfg;
}

fn run_query(db: pg::Pool&, t: pg::Ticket&, h: Host&) -> i64 {
    return pg::query(db, t, h, 1000, "SELECT id FROM t WHERE id > $1", 40) { r => print("row ", pg::int(r, 0)); };
}

fn main() {
    print("-- a query on a host of its own");
    var h = Host { .. };
    var db = pg::pool(config("10.1.2.3"), 1);
    var t = pg::Ticket { .. };
    var r = run_query(db, t, h);
    print("waits for the dial: ", r == pg::WAITS);
    // The host's events, in its own terms: the connection opens, then each answer arrives.
    pg::pool_opened(db, h.open);
    var steps = 0;
    var served = 0;
    while r == pg::WAITS && steps < 20 {
        steps++;
        loop {
            let key = pg::wake(db);
            if key == 0 { break; }
            r = run_query(db, t, h);
        }
        if r == pg::WAITS && h.flights > served {
            served = h.flights;
            pg::pool_data(db, h, h.open, answer(served)[..]);
        }
    }
    print("rows: ", r);
    check::that("the password went in cleartext", h.saw_password);

    print("-- handles that are not the pool's are none of its business");
    print("opened: ", pg::pool_opened(db, 999));
    print("data: ", pg::pool_data(db, h, 999, "x"));
    print("closed: ", pg::pool_closed(db, 999, pg::PoolFailure.Other));

    print("-- a host that refuses the dial");
    var h2 = Host { refuse: true, .. };
    var db2 = pg::pool(config("10.1.2.4"), 1);
    var t2 = pg::Ticket { .. };
    var r2 = run_query(db2, t2, h2);
    loop {
        let key = pg::wake(db2);
        if key == 0 { break; }
        r2 = run_query(db2, t2, h2);
    }
    print("failed: ", r2 == pg::FAILS, " ", t2.code[..], " ", db2.reason[..]);

    print("-- the time runs out");
    var h3 = Host { .. };
    var db3 = pg::pool(config("10.1.2.5"), 1);
    var t3 = pg::Ticket { .. };
    var r3 = run_query(db3, t3, h3);
    h3.now = 5000;
    pg::pool_tick(db3, h3.now);
    loop {
        let key = pg::wake(db3);
        if key == 0 { break; }
        r3 = run_query(db3, t3, h3);
    }
    print("failed: ", r3 == pg::FAILS, " ", t3.code[..], " ", db3.reason[..]);
}
