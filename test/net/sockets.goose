// The package's own sockets (net.goose): addresses, a listener on port 0, a
// non-blocking connect that polls writable, bytes both ways, EAGAIN with
// nothing to read, a peer that closes, and a connect that is refused.
import std;
import hash from "github.com/kaitk/goose-hash@v0.0.4";
import pg from "../../";
import pg.net;

fn show(text: const u8[:]) {
    let a, ok = pg::address(text);
    print(text, " -> ", ok, " len ", a.len);
}

fn ready(fd: i64, events: i16) -> bool {
    var fds: pg::PollFd[1] = [pg::PollFd { fd: fd as i32, events: events, .. }];
    return pg::wait_ready(fds[..], 2000) == 1 && (fds[0].revents & events) != 0;
}

fn main() {
    show("127.0.0.1:5432");
    show("localhost:80");
    show("10.0.0.256:1");
    show("1.2.3:4");
    show("db.example.com:5432");
    show("127.0.0.1:70000");
    show("unix:/tmp/s");
    show("unix:");

    let la, lok = pg::address("127.0.0.1:0");
    let listener = pg::listen_on(la);
    let port = pg::bound_port(listener);
    print("listening: ", listener >= 0 && port > 0);
    print("nothing to accept: ", pg::accept_one(listener) == -pg::EAGAIN);

    var text: u8[>..] = [];
    format(text, "127.0.0.1:", port);
    let ca, cok = pg::address(text[..]);
    let client, e = pg::connect_start(ca);
    print("connect started: ", client >= 0 && e == 0);
    print("client writable: ", ready(client, pg::POLLOUT));
    print("connected: ", pg::connect_result(client) == 0);
    print("accept ready: ", ready(listener, pg::POLLIN));
    let server = pg::accept_one(listener);
    print("accepted: ", server >= 0);

    var buf: u8[64] = [0; 64];
    print("nothing to read: ", pg::recv_some(server, buf[..]) == -pg::EAGAIN);
    print("sent: ", pg::send_some(client, "hello") == 5);
    print("server readable: ", ready(server, pg::POLLIN));
    let n = pg::recv_some(server, buf[..]);
    print("received: ", n, " ", buf[..n]);
    pg::send_some(server, "world!");
    print("client readable: ", ready(client, pg::POLLIN));
    let m = pg::recv_some(client, buf[..]);
    print("received: ", m, " ", buf[..m]);

    pg::close_fd(server);
    print("client sees the close: ", ready(client, pg::POLLIN));
    print("end of stream: ", pg::recv_some(client, buf[..]) == 0);
    pg::close_fd(client);
    pg::close_fd(listener);

    let r, re = pg::connect_start(ca);
    if re == 0 {
        ready(r, pg::POLLOUT);
        print("refused: ", pg::connect_result(r) == -pg::ECONNREFUSED);
        pg::close_fd(r);
    } else {
        print("refused: ", re == -pg::ECONNREFUSED);
    }
    print("clock moves: ", pg::clock_ms() > 0);
}
