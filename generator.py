#!/usr/bin/env python3
import json, time, random
from datetime import datetime

levels = ['info','warn','error']
caller = 'mvcc/hash.go:151'

while True:
    '''lvl = random.choice(levels)
    now = datetime.utcnow()
    rec = {
        "@timestamp":        now.timestamp(),
        "stream":            "stderr",
        "level":             lvl,
        "ts":                now.isoformat() + 'Z',
        "caller":            caller,
        "msg":               f"This is a fake {lvl} message",
        "hash":              random.randint(1_000_000_000, 1_999_999_999),
        "revision":          random.randint(40000, 50000),
        "compact_revision":  random.randint(40000, 50000)
    }
    print(json.dumps(rec), flush=True)
    # 2nd approach
    now = datetime.now()
    formatted_datetime = now.strftime("%Y/%m/%d %H:%M:%S")
    lvl = random.choice(levels)
    innode_num = random.randint(1000000, 3000000)
    fd_num = random.randint(1,99)

    rec = f'[{formatted_datetime}] [ {lvl}] [input:tail:tail.0] inotify_fs_remove(): inode={innode_num} watch_fd={fd_num}'
    print(rec, flush=True)
    time.sleep(15)'''

    # 3rd approach
    
    current_time = datetime.now().strftime("%H:%M:%S.%f")
    curr_time2 = datetime.now().strftime("%Y-%m-%d")
    lvl = random.choice(levels)
    # {\"level\":\"info\",\"ts\":\"2025-04-21T17:56:57.753397Z\",\"caller\":\"mvcc/index.go:214\",\"msg\":\"compact tree index\",\"revision\":60610}
    rec = {
        "level": lvl,
        "ts": f"{curr_time2}T{current_time}Z",
        "caller": caller,
        "msg": f"This is a fake {lvl} message",
        "hash": random.randint(1_000_000_000, 1_999_999_999),
        "revision": random.randint(40000, 50000),
        "compact_revision": random.randint(40000, 50000)
    }
    print(json.dumps(rec), flush=True)
    time.sleep(7)
