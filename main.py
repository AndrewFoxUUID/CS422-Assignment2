import os, sys, socket, json, struct, random, requests, numpy
from time import time, sleep
from matplotlib import pyplot

IPERF_URL="http://export.iperf3serverlist.net/listed_iperf3_servers.csv"
IPERF_PORT=5201
IPERF_STATE_START_TEST=1
IPERF_STATE_TEST_ACTIVE=2
IPERF_STATE_TEST_OVER=4
IPERF_STATE_EXCHANGE_PARAMS=9
IPERF_STATE_CREATE_STREAMS=10
IPERF_STATE_EXCHANGE_RESULTS=13
IPERF_STATE_DISPLAY_RESULTS=14
IPERF_STATE_FINISHED=16
IPERF_STATE_ACCESS_DENIED=255
ALNUMCHARS="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

def statetostr(state):
    if state == IPERF_STATE_START_TEST:
        return "Start Test"
    elif state == IPERF_STATE_TEST_ACTIVE:
        return "Test Active"
    elif state == IPERF_STATE_TEST_OVER:
        return "Test Over"
    elif state == IPERF_STATE_EXCHANGE_PARAMS:
        return "Exchange Params"
    elif state == IPERF_STATE_CREATE_STREAMS:
        return "Create Streams"
    elif state == IPERF_STATE_EXCHANGE_RESULTS:
        return "Exchange Results"
    elif state == IPERF_STATE_DISPLAY_RESULTS:
        return "Display Results"
    elif state == IPERF_STATE_FINISHED:
        return "Finished"
    elif state == IPERF_STATE_ACCESS_DENIED:
        return "Access Denied"
    else:
        return f"Unknown({state})"

def unitprint(v, u, n=12):
    neg = v < 0.0
    if neg:
        v = -v
    if v >= 1_000_000_000_000_000_000.0:
        s = f"{v/1_000_000_000_000_000_000.0:3.3f} E{u}"
    elif v >= 1_000_000_000_000_000.0:
        s = f"{v/1_000_000_000_000_000.0:3.3f} P{u}"
    elif v >= 1_000_000_000_000.0:
        s = f"{v/1_000_000_000_000.0:3.3f} T{u}"
    elif v >= 1_000_000_000.0:
        s = f"{v/1_000_000_000.0:3.3f} G{u}"
    elif v >= 1_000_000.0:
        s = f"{v/1_000_000.0:3.3f} M{u}"
    elif v >= 1_000.0:
        s = f"{v/1_000.0:3.3f} K{u}"
    elif v >= 1.0:
        s = f"{v:3.3f} {u}"
    elif v >= 0.001:
        s = f"{v*1_000.0:3.3f} m{u}"
    elif v >= 0.000_001:
        s = f"{v*1_000_000.0:3.3f} µ{u}"
    elif v >= 0.000_000_001:
        s = f"{v*1_000_000_000.0:3.3f} n{u}"
    elif v >= 0.000_000_000_001:
        s = f"{v*1_000_000_000_000.0:3.3f} p{u}"
    else:
        s = f"0.000 {u}"
    if neg:
        s = "-" + s
    if len(s) < n:
        s = " "*(n - len(s)) + s
    print(s, end="")

def gettcpinfo(client):
    if sys.platform == "darwin":
        raw = client.getsockopt(socket.IPPROTO_TCP, 262, 112)
        val = struct.unpack("<14I7Q", raw)
        snd_cwnd = val[6]
        snd_sbbytes = val[8]
        rttcur = val[10]
        srtt = val[11]
        rttvar = val[12]
        txbytes = val[15]
        txretransmitbytes = val[16]
        rxbytes = val[18]
        rxoutoforderbytes = val[19]
        return {
            "bytes_acked": txbytes - txretransmitbytes - snd_sbbytes, # bytes
            "snd_cwnd": snd_cwnd, # bytes
            "srtt": srtt, # ms
            "txretransmitbytes": txretransmitbytes, # bytes
            "rttvar": rttvar,
            "loss": txretransmitbytes / (txbytes - snd_sbbytes) # percent
        }
    else:
        raw = client.getsockopt(socket.IPPROTO_TCP, socket.TCP_INFO, 216)
        val = struct.unpack("<8B24I4Q6I4Q2I2Q", raw)
        return {
            "bytes_acked": val[34],
            "snd_cwnd": val[26],
            "srtt": val[23],
            "txretransmitbytes": val[49],
            "rttvar": val[24],
            "loss": val[49] / (val[34] + val[49])
        }

def checkstate(client, expectedstate=None):
    s = client.recv(1)
    if not s:
        raise ConnectionError("connection was found closed in checkstate call")
    state = struct.unpack("!B", s)[0]
    if expectedstate is not None and state != expectedstate:
        raise ConnectionError(f"connection state invalid, expected {statetostr(expectedstate)}, got {statetostr(state)}")
    return state

def setstate(client, state):
    client.sendall(struct.pack("!B", state))

summaries = []
def connect(host, duration=60, interval=0.5, ccalg="cubic", buflen=131072, pacing=1000, version="3.16"):
    times = []
    goodputs = []
    snd_cwnds = []
    rtts = []
    losses = []
    throughputs = []
    res = None
    cookie = "".join([random.choice(ALNUMCHARS) for _ in range(36)]).encode("ascii") + b'\x00'
    data = b'\xaa' * buflen
    if not hasattr(socket, "TCP_CONGESTION"):
        socket.TCP_CONGESTION = 13
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as control:
        control.settimeout(2)
        control.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if sys.platform != "darwin" or ccalg != "cubic":
            control.setsockopt(socket.IPPROTO_TCP, socket.TCP_CONGESTION, ccalg.encode("ascii") + b'\x00')
        control.connect((host, IPERF_PORT))
        control.sendall(cookie)
        checkstate(control, IPERF_STATE_EXCHANGE_PARAMS)
        conf = {
            "tcp": True,
            "omit": 0,
            "time": duration,
            "parallel": 1,
            "len": buflen,
            "pacing_timer": pacing,
            "client_version": version
        }
        confjson = json.dumps(conf).encode("utf-8")
        confpacket = struct.pack("!I", len(confjson)) + confjson
        control.sendall(confpacket)
        checkstate(control, IPERF_STATE_CREATE_STREAMS)

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client:
            client.settimeout(2)
            client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if sys.platform != "darwin" or ccalg != "cubic":
                client.setsockopt(socket.IPPROTO_TCP, socket.TCP_CONGESTION, ccalg.encode("ascii") + b'\x00')
            client.connect((host, IPERF_PORT))
            client.sendall(cookie)
            checkstate(control, IPERF_STATE_START_TEST)
            checkstate(control, IPERF_STATE_TEST_ACTIVE)

            start = time()
            now = start
            end = start + duration
            lastinterval = start
            nextinterval = start + interval
            sent = 0
            lastackedbytes, lastretransmittedbytes = 0, 0
            while now < end:
                if now >= nextinterval:
                    nextinterval += interval
                    tcpinfo = gettcpinfo(client)
                    times.append(now - start)
                    goodputs.append((tcpinfo["bytes_acked"] - lastackedbytes) / (now - lastinterval) * 8)
                    snd_cwnds.append(tcpinfo["snd_cwnd"])
                    rtts.append(tcpinfo["srtt"] * 1_000_000)
                    losses.append(tcpinfo["loss"])
                    throughputs.append((tcpinfo["bytes_acked"] + tcpinfo["txretransmitbytes"] - lastackedbytes - lastretransmittedbytes) / (now - lastinterval))
                    lastinterval = now
                    lastackedbytes, lastretransmittedbytes = tcpinfo["bytes_acked"], tcpinfo["txretransmitbytes"]
                client.sendall(data)
                sent += buflen
                now = time()
            final = now

            setstate(control, IPERF_STATE_TEST_OVER)
            checkstate(control, IPERF_STATE_EXCHANGE_RESULTS)
            resp = {
                "cpu_util_total": 0,
                "cpu_util_user": 0,
                "cpu_util_system": 0,
                "sender_has_retransmits": 0,
                "streams": [
                    {
                        "id": 1,
                        "bytes": sent,
                        "retransmits": -1,
                        "jitter": 0,
                        "errors": 0,
                        "omitted_errors": 0,
                        "packets": 0,
                        "omitted_packets": 0,
                        "start_time": 0,
                        "end_time": final - start
                    }
                ]
            }
            respjson = json.dumps(resp).encode("utf-8")
            resppacket = struct.pack("!I", len(respjson)) + respjson
            control.sendall(resppacket)
            resn = control.recv(4)
            if len(resn) != 4:
                raise ConnectionError("error retrieving result length from host")
            resn = struct.unpack("!I", resn)[0]
            res = control.recv(resn)
            if len(res) != resn:
                raise ConnectionError("error retrieving result from host")
            res = json.loads(res)
            setstate(control, IPERF_STATE_DISPLAY_RESULTS)
    if (res):
        summaries.append([host, numpy.min(goodputs), numpy.percentile(goodputs, [50])[0], numpy.mean(goodputs), numpy.percentile(goodputs, [95])[0]])
        
        pyplot.scatter(times, goodputs)
        pyplot.xlabel("time (s)")
        pyplot.ylabel("goodput (bits/s)")
        pyplot.title("goodput vs time for " + host)
        pyplot.savefig(f"plots/{ccalg}_goodput_{host}.png")
        pyplot.close()

        pyplot.scatter(times, snd_cwnds)
        pyplot.xlabel("time (s)")
        pyplot.ylabel("snd_cwnd (B)")
        pyplot.title("snd_cwnd vs time for " + host)
        pyplot.savefig(f"plots/{ccalg}_sndcwnd_{host}.png")
        pyplot.close()

        pyplot.scatter(times, rtts)
        pyplot.xlabel("time (s)")
        pyplot.ylabel("RTT (ms)")
        pyplot.title("RTT vs time for " + host)
        pyplot.savefig(f"plots/{ccalg}_rtt_{host}.png")
        pyplot.close()

        pyplot.scatter(times, losses)
        pyplot.xlabel("time (s)")
        pyplot.ylabel("loss (# retransmitted/# transmitted)")
        pyplot.title("loss vs time for " + host)
        pyplot.savefig(f"plots/{ccalg}_loss_{host}.png")
        pyplot.close()

        pyplot.scatter(times, throughputs)
        pyplot.xlabel("time (s)")
        pyplot.ylabel("throughput (B/s)")
        pyplot.title("throughput vs time for " + host)
        pyplot.savefig(f"plots/{ccalg}_throughput_{host}.png")
        pyplot.close()

        pyplot.scatter(goodputs, [snd_cwnd*8 for snd_cwnd in snd_cwnds])
        pyplot.xlabel("goodput (bits/s)")
        pyplot.ylabel("snd_cwnd (bits)")
        pyplot.title("snd_cwnd vs goodput for " + host)
        pyplot.savefig(f"plots/{ccalg}_sndcwnd_vs_goodput_{host}.png")
        pyplot.close()

        pyplot.scatter(goodputs, [rtt*1000 for rtt in rtts])
        pyplot.xlabel("goodput (bits/s)")
        pyplot.ylabel("RTT (s)")
        pyplot.title("RTT vs goodput for " + host)
        pyplot.savefig(f"plots/{ccalg}_rtt_vs_goodput_{host}.png")
        pyplot.close()

        pyplot.scatter(goodputs, losses)
        pyplot.xlabel("goodput (bits/s)")
        pyplot.ylabel("loss (# retransmitted/# transmitted)")
        pyplot.title("loss vs goodput for " + host)
        pyplot.savefig(f"plots/{ccalg}_loss_vs_goodput_{host}.png")
        pyplot.close()


if __name__ == "__main__":
    if "help" in "".join(sys.argv):
        print("usage: python3 main.py {-f</path/to/server_list_file:str> | -n<#destinations:int=10>} <duration:int=60> <interval:float=0.5> <ccalg:str=cubic>")
        sys.exit(0)

    serverlistfile = None
    n = 10
    duration = 60
    interval = 0.5
    ccalg = "cubic"
    repetitions = 1
    if len(sys.argv) > 1:
        if sys.argv[1][:2] == "-f":
            serverlistfile = sys.argv[1][2:]
        elif sys.argv[1][:2] == "-n":
            n = int(sys.argv[1][2:])
        else:
            print("usage: python3 main.py {-f</path/to/server_list_file:str> | -n<#destinations:int=10>} <duration:int=60> <interval:float=0.5> <ccalg:str=cubic> <repetitions:int=1>")
            sys.exit(0)
    if len(sys.argv) > 2:
        duration = int(sys.argv[2])
    if len(sys.argv) > 3:
        interval = float(sys.argv[3])
    if len(sys.argv) > 4:
        ccalg = sys.argv[4]
        if ccalg not in ["cubic", "bbr", "reno"]:
            print(f"unknown congestion control algorithm \"{ccalg}\"")
            sys.exit(1)
    if len(sys.argv) > 5:
        repetitions = int(sys.argv[5])
        
    validhosts = []
    if serverlistfile is None:
        response = requests.get(IPERF_URL)
        response.raise_for_status()
        raw = response.text.strip().splitlines()[1:]
        for i in range(len(raw)):
            row = raw[i].split(",")
            if len(row) != 7:
                continue
            validhosts.append(row[0])
    else:
        with open(serverlistfile) as f:
            for line in f.readlines():
                if len(line) > 0:
                    validhosts.append(line.strip())

    completed = 0
    failed = []
    try:
        os.mkdir("plots")
    except FileExistsError:
        pass
    while completed < n and len(validhosts) > 0:
        hosti = random.choice(range(len(validhosts)))
        host = validhosts[hosti]
        validhosts.pop(hosti)
        print(f"testing {host} ... ", end="", flush=True, file=sys.stderr)

        try:
            connect(host, duration=duration, interval=interval, ccalg=ccalg)
            completed += 1
            print("success", file=sys.stderr)
        except Exception as e:
            failed.append(host)
            print(e, file=sys.stderr)
    print(f"\nsummaries for {ccalg}:")
    print("                    host             minimum              median                mean    95-th percentile")
    while len(summaries) > 0:
        summary = summaries[0]
        multiplicity = 1
        summaries.pop(0)
        for i in list(range(len(summaries)))[::-1]:
            if summaries[i][0] != summary[0]:
                continue
            summary[1] += summaries[i][1]
            summary[2] += summaries[i][2]
            summary[3] += summaries[i][3]
            summary[4] += summaries[i][4]
            multiplicity += 1
            summaries.pop(i)
        summary[1] /= multiplicity
        summary[2] /= multiplicity
        summary[3] /= multiplicity
        summary[4] /= multiplicity
        
        print(f"{summary[0]:>24.24}    ", end="")
        unitprint(summary[1], "bits/s", 16)
        print("    ", end="")
        unitprint(summary[2], "bits/s", 16)
        print("    ", end="")
        unitprint(summary[3], "bits/s", 16)
        print("    ", end="")
        unitprint(summary[4], "bits/s", 16)
        print()
    if len(failed) > 0:
        print("\nthe following hosts were not queried successfully: " + ", ".join(failed), file=sys.stderr)
