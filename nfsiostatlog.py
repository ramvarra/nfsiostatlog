'''
Generate json formated lines for nfsiostat
'''
from __future__ import print_function

import os, re, sys
import json
from datetime import datetime, timedelta
import subprocess

def is_float(s):
    return re.match(r'[\d\.]+$', s)

def make_metrics(prev_line, data):
    data = map(float, data)
    if prev_line.startswith(' '):
        cols = ('ops_sec', 'rpc_bklog')
    else:
        if prev_line.startswith('read:'):
            prefix = 'rd_'
        elif prev_line.startswith('write:'):
            prefix = 'wr_'
        else:
            raise Exception("Invalid prev_line '%s' with data: '%s'" % (prev_line, data))
        cols = ('ops_sec', 'kb_sec', 'kb_op', 'retrans', 'retrans_pct', 'avg_rtt_ms', 'avg_exe_ms')
        cols = [prefix + c for c in cols]
    
    d = dict(zip(cols, data))
    return d

def get_recs_from_text(log_text):                    
    recs = []
    rec = {}
    lines = [re.sub(r'[()%]+', '', l) for l in log_text.split('\n') if l.strip()]

    for i, line in enumerate(lines):
        toks = re.split(r'\s+', line)
        if toks[1:3] == ['mounted', 'on']:
            if rec:
                recs.append(rec)
            rec = {'vol': toks[0], 'mnt': toks[3]}
        elif len(toks) > 2 and toks[0] == '' and is_float(toks[1]) and is_float(toks[2]):
            rec.update(make_metrics(lines[i-1], toks[1:]))
    if rec:
        recs.append(rec)
    return recs

def post_process(recs, init_ts, interval):
    first_mnt = recs[0]['mnt']
    for i in range(1, len(recs)):
        if recs[i]['mnt'] == first_mnt:
            break
    ts = init_ts 
    for r in recs[1:]:
        if r['mnt'] == first_mnt:
            ts += interval
        r['ts'] = ts.isoformat()
    return recs[i:]

def main(interval_secs, num_samples, out_fp):
    cmd = 'nfsiostat %d %d' % (interval_secs, num_samples)
    ts_start = datetime.now()
    interval = timedelta(seconds=interval_secs)
    log_text = subprocess.check_output(cmd.split()).decode()
    recs = get_recs_from_text(log_text) 
    recs = post_process(recs, ts_start, interval)
    for r in recs:
        json.dump(r, out_fp)
        print('', file=out_fp)

# ---------------------------------------------------------------
if __name__ == '__main__':
    MAX_LOG_SIZE = 4 * 1024 * 1024
    interval_secs = 15
    num_samples = 7
    log_file = None
    if len(sys.argv) > 1:
        if len(sys.argv) > 2:
            interval_secs = int(sys.argv[1])
            num_samples = int(sys.argv[2]) + 1
            if num_samples < 2:
                print('Invalid num_samples %d' % num_samples)
                sys.exit(1)
            if interval_secs < 5:
                print('Invalid interval_secs %d (must be > 5)' % interval_secs)
                sys.exit(1)
        if len(sys.argv) == 4:
            log_file = sys.argv[3]
        if len(sys.argv) > 4 or len(sys.argv) == 2:
            print('ERROR: unexpected number of args : %s' % sys.argv)
            print('Usage: %s [<interval> <num_samples>]')
            sys.exit(1)

    while True:
        if log_file:
            if os.path.exists(log_file):
                size = os.path.getsize(log_file)
                if size > MAX_LOG_SIZE:
                    bak_file = log_file + '.bak'
                    os.rename(log_file, bak_file)
            out_fp = open(log_file, 'a+')
        else:
            out_fp = sys.stdout
        try:
            main(interval_secs, num_samples, out_fp)
        finally:
            if log_file:
                out_fp.close()