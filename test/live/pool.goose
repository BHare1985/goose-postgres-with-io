// The pool (pool.goose) on the package's own event loop, against a server that
// scripts/live.sh starts:
//
//   pool postgres://bench:bench@127.0.0.1:55432/benchmark
//
// One request; then 40 requests at once on a pool of 4 connections, each
// asking for a different number of rows, so the queue is used and every answer
// is checked against its own request; then a request that fails and one after
// it on the same pool; then a pool whose server does not answer in time.
import std;
import os;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "../../";
import pg.eventloop;
import pg.looppool;
import pg.pool;

let REQUESTS = 40;

struct App {
    tickets: pg::Ticket[REQUESTS] = [pg::Ticket { .. }; REQUESTS],
    rows: i64[REQUESTS] = [0; REQUESTS],
    ok: i64[REQUESTS] = [0; REQUESTS],
    started: i64 = 0,
    finished: i64 = 0,
    wanted: i64 = 0,
    sql: u8[..96],
    failed_code: u8[..5],
    pool: pg::Pool,
}

// Request n asks for the n lowest ids, and checks that it got exactly those.
fn route(app: App&, n: i64, lp: pg::Loop&) {
    let t .= app.tickets[n];
    guard t.state != pg::TICKET_DONE else { return; }
    var next = 1;
    let r = pg::query(app.pool, t, lp, 10000, app.sql[..], n + 1) { r =>
        if pg::int(r, 0) != next { app.ok[n] = -1; }
        next++;
    };
    if r == pg::WAITS { return; }
    if r == pg::FAILS {
        app.ok[n] = -2;
        app.failed_code.clear();
        app.failed_code.append(t.code[..]);
    } else {
        app.rows[n] = r;
        if app.ok[n] == 0 { app.ok[n] = 1; }
    }
    app.finished++;
}

fn event(app: App&, lp: pg::Loop&, ev: pg::LoopEvent) {
    pg::pool_event(app.pool, lp, ev);
    loop {
        let key = pg::wake(app.pool);
        if key == 0 { break; }
        for n in app.started { if app.tickets[n].key == key { route(app, n, lp); } }
    }
    if app.finished == app.wanted { lp.stop(); }
}

// Runs count requests of sql at once on a new pool of size connections to cfg's
// server and a new loop, and says how each ended.
fn phase(label: const u8[:], cfg: pg::Config, size: i64, sql: const u8[:], count: i64) {
    print("-- ", label);
    var app = App { pool: pg::pool(cfg, size), .. };
    app.sql.append(sql);
    app.wanted = count;
    var lp = pg::Loop { .. };
    for n in count {
        app.started = n + 1;
        route(app, n, lp);
    }
    lp.serve() { lp, ev => event(app, lp, ev); };
    var rows_ok = 0;
    var failed = 0;
    for n in count {
        if app.ok[n] == 1 && app.rows[n] == n + 1 { rows_ok++; }
        if app.ok[n] == -2 { failed++; }
    }
    print("requests: ", count, ", each got its own rows in order: ", rows_ok, ", failed: ", failed);
    if failed > 0 { print("failed with ", app.failed_code[..], ": ", app.pool.reason[..]); }
}

fn main() {
    var url = "";
    var k = 0;
    for a in args() {
        if k == 1 { url = a; }
        k++;
    }
    let cfg, ok = pg::parse_url(url);
    guard ok else { abort("usage: pool postgres://..."); }

    let sql = "SELECT id FROM items WHERE id <= $1 ORDER BY id";
    phase("one request", cfg, 4, sql, 1);
    phase("40 requests on 4 connections", cfg, 4, sql, REQUESTS);
    phase("a request that fails", cfg, 4, "SELECT id FROM no_such_table WHERE id <= $1", 1);
    phase("40 requests on one connection", cfg, 1, sql, REQUESTS);
    var bad = cfg;
    bad.port = 1;
    phase("a server that is not there", bad, 2, sql, 1);
}
