// The event loop driven one pass at a time (step), as a host with a loop of its own would
// drive it: the host's own work runs between passes, a pass waits no longer than the host
// says, and the loop's events are delivered during the passes. A client and a server on one
// loop exchange a message; the host counts its passes and stops the loop.
import std;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "../../";
import pg.eventloop;

struct App { listener: pg::Link = pg::Link { .. }, client: pg::Link = pg::Link { .. }, server: pg::Link = pg::Link { .. }, got: i64 = 0 }

fn event(app: App&, lp: pg::Loop&, ev: pg::LoopEvent) {
    match ev {
        Opened e => {
            if pg::same(e.link, app.client) { lp.send(e.link, "ping"); }
            else if pg::is_valid(e.listener) { app.server = e.link; }
        },
        Data e => {
            if pg::same(e.link, app.server) { lp.send(e.link, "pong"); }
            else { app.got += e.bytes.len; lp.stop(); }
        },
        _ => {},
    }
}

fn main() {
    var app = App { .. };
    var lp = pg::Loop { .. };
    let l, e = lp.listen("127.0.0.1:0");
    app.listener = l;
    var addr: u8[>..] = [];
    format(addr, "127.0.0.1:", lp.listener_port(l));
    app.client = lp.dial(addr[..], 2000);

    var passes = 0;
    var host_work = 0;
    // The host's loop: its own work, then one pass of this loop that waits at most 5 ms.
    var more = true;
    while more && passes < 1000 {
        host_work++;
        passes++;
        more = lp.step(5) { lp, ev => event(app, lp, ev); };
    }
    print("the exchange finished: ", app.got == 4);
    print("the host's work ran between passes: ", host_work > 1);
    print("the loop finished: ", lp.draining());
    print("a step after the end returns false: ", !lp.step(5) { lp, ev => event(app, lp, ev); });
}
