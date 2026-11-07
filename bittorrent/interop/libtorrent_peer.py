#!/usr/bin/env python3
"""A libtorrent-rasterbar peer for TestClientLibtorrent (see libtorrent.sh).

It seeds or downloads one torrent on 127.0.0.1, over TCP or uTP alone,
with encryption allowed or forced, prints "PORT <port>" once it listens,
"DONE" once a download is complete, and runs until killed or until the
timeout.

usage: libtorrent_peer.py seed|leech <torrent file|magnet> <save dir>
       [--peer host:port] [--utp] [--require] [--timeout secs]
"""
import argparse
import sys
import time

import libtorrent as lt


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["seed", "leech"])
    ap.add_argument("source")
    ap.add_argument("save")
    ap.add_argument("--peer")
    ap.add_argument("--utp", action="store_true")
    ap.add_argument("--require", action="store_true")
    ap.add_argument("--timeout", type=float, default=120)
    a = ap.parse_args()

    enc = lt.enc_policy.forced if a.require else lt.enc_policy.enabled
    level = lt.enc_level.rc4 if a.require else lt.enc_level.both
    s = lt.session({
        "listen_interfaces": "127.0.0.1:0",
        "enable_dht": False,
        "enable_lsd": False,
        "enable_upnp": False,
        "enable_natpmp": False,
        "enable_outgoing_utp": a.utp,
        "enable_incoming_utp": a.utp,
        "enable_outgoing_tcp": not a.utp,
        "enable_incoming_tcp": not a.utp,
        "out_enc_policy": enc,
        "in_enc_policy": enc,
        "allowed_enc_level": level,
        "allow_multiple_connections_per_ip": True,
        "alert_mask": lt.alert.category_t.error_notification | lt.alert.category_t.peer_notification,
    })
    if a.source.startswith("magnet:"):
        atp = lt.parse_magnet_uri(a.source)
    else:
        atp = lt.add_torrent_params()
        atp.ti = lt.torrent_info(a.source)
    atp.save_path = a.save
    h = s.add_torrent(atp)
    print("PORT %d" % s.listen_port(), flush=True)
    if a.peer:
        host, port = a.peer.rsplit(":", 1)
        h.connect_peer((host, int(port)))

    deadline = time.time() + a.timeout
    done = False
    last = 0
    while time.time() < deadline:
        for al in s.pop_alerts():
            name = type(al).__name__
            if (al.category() & lt.alert.category_t.error_notification) or "hash" in name or "ban" in name:
                print("ALERT %s: %s" % (name, al.message()), flush=True)
        st = h.status()
        if a.mode == "leech" and not done and st.is_seeding:
            print("DONE", flush=True)
            done = True
        if a.peer and st.num_peers == 0 and not st.is_seeding and time.time() - last > 2:
            last = time.time()
            h.connect_peer((host, int(port)))
        if time.time() - last > 2:
            print("STATUS %s %.3f peers=%d up=%d" % (st.state, st.progress, st.num_peers, st.total_payload_upload), flush=True)
            last = time.time()
        time.sleep(0.1)
    return 0 if (done or a.mode == "seed") else 1


if __name__ == "__main__":
    sys.exit(main())
