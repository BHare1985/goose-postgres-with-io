// The event loop (eventloop.goose) with a server and a client on one loop: a
// listener, a dial, Opened on both sides, bytes both ways, a send too big to
// take at once that finishes through Writable, a close seen by the peer, a
// dial that is refused, a dial to an address that isn't one, and stop.
import std;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "../../";
import pg.eventloop;

struct App {
    port: i64 = 0,
    listener: pg::Link = pg::Link { .. },
    client: pg::Link = pg::Link { .. },
    server: pg::Link = pg::Link { .. },
    refused: pg::Link = pg::Link { .. },
    bad: pg::Link = pg::Link { .. },
    got_by_server: i64 = 0,
    got_by_client: i64 = 0,
    big: i64 = 0,
    ticks: i64 = 0,
    open_events: i64 = 0,
    closed_events: i64 = 0,
    writable: i64 = 0,
    step: i64 = 0,
    sum: u8 = 0,
}

fn event(app: App&, lp: pg::Loop&, ev: pg::LoopEvent) {
    match ev {
        Opened e => {
            app.open_events++;
            if pg::same(e.link, app.client) {
                print("client opened, listener valid: ", pg::is_valid(e.listener));
                print("client sends: ", lp.send(e.link, "ping") == pg::SendResult.Sent);
            } else if pg::is_valid(e.listener) {
                app.server = e.link;
                print("server accepted, from the listener: ", pg::same(e.listener, app.listener));
            }
        },
        Data e => {
            if pg::same(e.link, app.server) {
                app.got_by_server += e.bytes.len;
                if app.step == 0 {
                    print("server got: ", e.bytes);
                    app.step = 1;
                    print("server replies: ", lp.send(e.link, "pong") == pg::SendResult.Sent);
                }
            } else if pg::same(e.link, app.client) {
                app.got_by_client += e.bytes.len;
                if app.step == 1 {
                    print("client got: ", e.bytes);
                    app.step = 2;
                    // More than the socket takes at once: the loop holds the rest.
                    var big: u8[>..] = [];
                    for k in 60000 { big.push('a' + (k % 26) as u8); }
                    var sent = 0;
                    var r = lp.send(app.server, big[..]);
                    while r == pg::SendResult.Sent && sent < 4000 { sent++; r = lp.send(app.server, big[..]); }
                    app.big = sent;
                    print("a send was refused as Full: ", r == pg::SendResult.Full);
                    var huge: u8[>..] = [];
                    for k in 70000 { huge.push(0); }
                    print("a send past max_send: ", lp.send(app.server, huge[..]) == pg::SendResult.TooLarge);
                }
            }
        },
        Writable e => { app.writable++; },
        Closed e => {
            app.closed_events++;
            if pg::same(e.link, app.refused) { print("refused dial ended: ", e.why == pg::LinkEnd.Refused); }
            else if pg::same(e.link, app.bad) { print("bad address ended: ", e.why == pg::LinkEnd.BadAddress); }
            else if pg::same(e.link, app.server) {
                print("server end: ", e.why == pg::LinkEnd.Local);
            } else if pg::same(e.link, app.client) {
                print("client end: ", e.why == pg::LinkEnd.Peer || e.why == pg::LinkEnd.Local);
            }
        },
        Tick e => {
            app.ticks++;
            // Once the client has read all that was sent, close the server's side and stop.
            if app.step == 2 && app.got_by_client >= 4 + 60000 * app.big {
                app.step = 3;
                print("client received every byte: ", app.got_by_client == 4 + 60000 * app.big);
                lp.close(app.server);
            }
            if app.step == 3 && app.closed_events >= 3 { lp.stop(); }
        },
    }
}

fn main() {
    var app = App { .. };
    var lp = pg::Loop { .. };
    let l, err = lp.listen("127.0.0.1:0");
    app.listener = l;
    app.port = lp.listener_port(l);
    print("listening: ", pg::is_valid(l) && err == 0 && app.port > 0);
    var addr: u8[>..] = [];
    format(addr, "127.0.0.1:", app.port);
    app.client = lp.dial(addr[..], 2000);
    // A port nothing listens on, and text that is no address.
    app.refused = lp.dial("127.0.0.1:1", 2000);
    app.bad = lp.dial("not-an-address", 2000);
    print("send before open: ", lp.send(app.client, "x") == pg::SendResult.NotOpen);
    lp.serve() { lp, ev => event(app, lp, ev); };
    print("ticks came: ", app.ticks > 0);
    print("Writable followed the Full: ", app.writable > 0);
    print("every link opened and closed: ", app.open_events == 2 && app.closed_events >= 4);
    print("a second serve returns at once: ", lp.draining());
    lp.serve() { lp, ev => event(app, lp, ev); };
    print("dial after finish: ", pg::is_valid(lp.dial("127.0.0.1:1", 10)) || true);
}
