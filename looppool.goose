// The package's own event loop (eventloop.goose) as a host of the pool (pool.goose): the four
// operations the pool asks of its host, and pool_event, which turns the loop's events into the
// four calls the pool takes. A program that runs the pool on its own loop imports this module
// beside pg.eventloop and pg.pool; a program with another loop imports pg.pool alone.
//
//   lp.serve() { lp, ev =>
//       pg::pool_event(db, lp, ev);
//       loop { let key = pg::wake(db); if key == 0 { break; } ... }
//   };
namespace pg;

import std;
import .eventloop;
import .pool;

// A link as the pool's handle: its slot and generation in one nonzero i64.
fn handle_of(l: Link) -> i64 { if is_valid(l) { ((l.gen as i64) << 32) | l.slot } else { 0 } }

fn link_from(h: i64) -> Link { Link { slot: (h & 0xffffffff) as u32, gen: (h >> 32) as u32 } }

fn host_dial(lp: Loop&, where: const u8[:], ms: i64) -> i64 { handle_of(lp.dial(where, ms)) }

fn host_send(lp: Loop&, h: i64, bytes: const u8[:]) -> bool { lp.send(link_from(h), bytes) == SendResult.Sent }

fn host_close(lp: Loop&, h: i64) { lp.close(link_from(h)); }

fn host_now_ms(lp: Loop&) -> i64 { lp.now_ms() }

fn failure_of(why: LinkEnd) -> PoolFailure {
    match why {
        Refused => PoolFailure.Refused,
        TimedOut => PoolFailure.TimedOut,
        Unreachable => PoolFailure.Unreachable,
        BadAddress => PoolFailure.BadAddress,
        _ => PoolFailure.Other,
    }
}

// Gives the pool one event of the loop. True if it was the pool's; then take wake until it gives 0.
fn pool_event(p: Pool&, lp: Loop&, ev: LoopEvent) -> bool {
    match ev {
        Tick t => { pool_tick(p, t.now_ms); return false; },
        Opened e => { return pool_opened(p, handle_of(e.link)); },
        Data e => { return pool_data(p, lp, handle_of(e.link), e.bytes); },
        Closed e => { return pool_closed(p, handle_of(e.link), failure_of(e.why)); },
        _ => { return false; },
    }
}
