// The event loop over a Unix socket: a listener at a path, a second listener
// at the same path that fails without touching the first, a dial, bytes both
// ways, and the path removed when the listener ends, so the next run can listen again.
import std;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "../../";
import pg.eventloop;

let PATH = "unix:/tmp/goose-postgres-with-io-test.sock";

struct App { listener: pg::Link = pg::Link { .. }, client: pg::Link = pg::Link { .. }, server: pg::Link = pg::Link { .. }, replies: i64 = 0 }

fn event(app: App&, lp: pg::Loop&, ev: pg::LoopEvent) {
    match ev {
        Opened e => {
            if pg::same(e.link, app.client) { lp.send(e.link, "ping"); }
            else if pg::is_valid(e.listener) { app.server = e.link; print("accepted on the path: ", pg::same(e.listener, app.listener)); }
        },
        Data e => {
            if pg::same(e.link, app.server) {
                print("server got: ", e.bytes);
                lp.send(e.link, "pong");
            } else {
                print("client got: ", e.bytes);
                app.replies++;
                lp.stop();
            }
        },
        _ => {},
    }
}

fn main() {
    var app = App { .. };
    var lp = pg::Loop { .. };
    let l, e = lp.listen(PATH);
    print("listening: ", pg::is_valid(l) && e == 0);
    app.listener = l;
    let l2, e2 = lp.listen(PATH);
    print("a second listener at the same path: ", !pg::is_valid(l2), " in use: ", e2 == pg::EADDRINUSE);
    app.client = lp.dial(PATH, 2000);
    lp.serve() { lp, ev => event(app, lp, ev); };
    print("a reply came: ", app.replies == 1);

    var lp2 = pg::Loop { .. };
    let l3, e3 = lp2.listen(PATH);
    print("the path was removed when the listener ended: ", pg::is_valid(l3) && e3 == 0);
    lp2.stop();
    lp2.serve() { lp, ev => {}; };
}
