#!/usr/bin/env python

import sys
import re

class MemberInfo(object):
    __slots__ = ('fail_ts', 'first_suspect_ts', 'first_dead_ts',
                 'last_dead_ts', 'dead_update_count')

if len(sys.argv) != 2:
    print "Usage: get-swim-stats.pl <swim log file>"
    exit(1)

swim_log = sys.argv[1]
member_info = {}

# ssg debug output looks like "timestamp <member>"
swim_dbg_pattern = re.compile('^\d+.\d+ <\d+>')

with open(swim_log, 'rU') as f:
    for line in f:
        if not swim_dbg_pattern.match(line):
            continue

        fields = line.split(" ")
        ts = float(fields[0])
        member = int(fields[1][1:-2])
        update = fields[2:]

        # FIXME
        if update[0] == "group" and update[1] == "lookup" and update[2] == "successful":
            group_size = update[3][6:-2]

        # FIXME
        if update[0] != "swim" or update[1] != "member":
            continue
        target_member = int(update[2])
        target_member_status = update[3]

	if target_member not in member_info:
            member_info[target_member] = MemberInfo()

        # TODO: consider alive updates & incarnation numbers?
        if target_member_status == "FAIL":
            if not hasattr(member_info[target_member], 'fail_ts'):
                member_info[target_member].fail_ts = ts
        elif target_member_status == "SUSPECT":
            if not hasattr(member_info[target_member], 'first_suspect_ts'):
                member_info[target_member].first_suspect_ts = ts
        elif target_member_status == "DEAD":
            if not hasattr(member_info[target_member], 'first_dead_ts'):
                member_info[target_member].first_dead_ts = ts
                member_info[target_member].dead_update_count = 0
            member_info[target_member].dead_update_count += 1
            member_info[target_member].last_dead_ts = ts

for member in member_info:
    if not hasattr(member_info[member], 'fail_ts'):
        member_info[member].fail_ts = float('nan')
    print "Member %d fail info:" % member
    print "\tfail time = %f" % member_info[member].fail_ts
    print "\tfirst suspect time = %f" % member_info[member].first_suspect_ts
    print "\tfirst dead time = %f" % member_info[member].first_dead_ts
    print "\tlast dead time = %f (%d)" % (member_info[member].last_dead_ts, member_info[member].dead_update_count)
