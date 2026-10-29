#!/usr/bin/env python3
"""Read-only validation of one trial's diag net records (schema version 1).

Usage: validate-diag-net.py RUN_LOG --nodes 13 --duration-s 30 --interval-s 1
                           [--manifest trial.manifest.json]

Use alongside validate-samples.py and the trial acceptance checks, not instead
of them. Complete means a well-formed, gap-free capture, not a healthy network
or proof that every connection existed. Missing opposite endpoints are listed,
not interpreted as zero queues. No simultaneity or causal ordering is inferred.
Exit: 0 complete, 1 invalid, 4 incomplete (missing/partial/ambiguous rounds,
missed cadence, diagnostic failure, or unmatched established sockets). One
ordinary `diag net-status` line is required per node and cross-checked against
any worker terminal marker. A trailing unfinished round is incomplete even
without a marker; an unfinished round followed by later rounds is invalid.
An aborted trial can have incomplete records; the resource-stop validator
checks its cause.
"""
import argparse
from collections import defaultdict
from datetime import datetime
import json
import math
import re
import sys

PREFIX = re.compile(r'^(?:\S+ \S+ \[(?P<host>[^\]\s]+)\] )?brb bench: node (?P<node>\d+) diag net (?P<data>.*)$')
STATUS_PREFIX = re.compile(r'^(?:\S+ \S+ \[(?P<host>[^\]\s]+)\] )?brb bench: node (?P<node>\d+) diag net-status status=(?P<status>ok|not_started|shutdown_timeout|output_unavailable|output_limit|unavailable_sink)$')
BASE = {'version', 'round', 'kind', 'phase', 'start_ms', 'end_ms', 'anchor_utc'}
SOCKET = {'dialer', 'acceptor', 'client_port', 'peer', 'role', 'state', 'tx_queue',
          'rx_queue', 'timer_active', 'timer_ticks', 'unrecovered_rto', 'zero_window_probes',
          'generation', 'lifecycle', 'reconnect', 'ambiguous', 'field_status'}
QUEUES = {'tx_queue', 'rx_queue', 'timer_active', 'timer_ticks', 'unrecovered_rto', 'zero_window_probes'}
STATUSES = {'ok', 'read_error', 'parse_error', 'byte_limit', 'entry_limit', 'socket_limit'}
SOURCES = ['tcp', 'tcp6', 'snmp', 'netstat']
COUNTERS = ['RetransSegs', 'OutSegs', 'TCPTimeouts']


def strict_object(pairs):
    out = {}
    for k, v in pairs:
        if k in out:
            raise ValueError('duplicate JSON key')
        out[k] = v
    return out


def keys(obj, expected):
    if type(obj) is not dict or set(obj) != expected:
        raise ValueError('unexpected schema fields')


def integer(v, lo=0, hi=2**64-1):
    if type(v) is not int or not lo <= v <= hi:
        raise ValueError('integer outside range')


def parse_record(raw, node, nodes):
    r = json.loads(raw, object_pairs_hook=strict_object,
                   parse_constant=lambda _: (_ for _ in ()).throw(ValueError('nonfinite JSON')))
    if type(r) is not dict or r.get('kind') not in {'socket', 'round', 'missed', 'terminal'}:
        raise ValueError('unknown record kind')
    payload = {'socket': 'socket', 'round': 'summary', 'missed': 'missed', 'terminal': 'terminal'}[r['kind']]
    keys(r, BASE | {payload})
    if r['version'] != 1 or type(r['version']) is not int:
        raise ValueError('unsupported version')
    integer(r['round']); integer(r['start_ms']); integer(r['end_ms'], r['start_ms'])
    if r['phase'] not in {'initial', 'periodic', 'final'}:
        raise ValueError('unknown phase')
    if type(r['anchor_utc']) is not str or not r['anchor_utc'].endswith('Z'):
        raise ValueError('UTC anchor required')
    datetime.fromisoformat(r['anchor_utc'].replace('Z', '+00:00'))
    if r['kind'] == 'socket':
        s = r['socket']; keys(s, SOCKET)
        for k in ('dialer', 'acceptor', 'peer'):
            integer(s[k], 0, nodes)
        integer(s['client_port'], 1, 65535); integer(s['state'], 1, 12)
        integer(s['generation'])
        for k in ('reconnect', 'ambiguous'):
            if type(s[k]) is not bool:
                raise ValueError('boolean required')
        if s['dialer'] == s['acceptor'] or s['role'] not in {'dialed', 'accepted'}:
            raise ValueError('socket direction')
        if s['role'] == 'dialed':
            valid = s['dialer'] == node and s['acceptor'] == s['peer'] > 0
        else:
            valid = s['acceptor'] == node and s['dialer'] == s['peer']
        if not valid or (s['peer'] == 0 and not s['ambiguous']):
            raise ValueError('socket node binding')
        if s['lifecycle'] not in {'unknown', 'first', 'observed', 'reappeared_or_replaced'}:
            raise ValueError('unknown lifecycle')
        if s['lifecycle'] == 'unknown' and s['generation'] != 0:
            raise ValueError('unknown generation')
        if s['lifecycle'] != 'unknown' and s['generation'] < 1:
            raise ValueError('missing generation')
        if s['lifecycle'] == 'first' and s['generation'] != 1:
            raise ValueError('first generation')
        if s['lifecycle'] == 'reappeared_or_replaced' and not s['reconnect']:
            raise ValueError('missing reconnect flag')
        if s['state'] == 1:
            if s['field_status'] != 'ok':
                raise ValueError('established fields unavailable')
            for k in QUEUES:
                integer(s[k], hi=255 if k == 'timer_active' else 2**64-1)
        elif s['field_status'] != 'state_only' or any(s[k] is not None for k in QUEUES):
            raise ValueError('non-established fields must be null')
    elif r['kind'] == 'missed':
        if r['phase'] == 'initial':
            raise ValueError('initial round cannot have missed slots')
        m = r['missed']; keys(m, {'first', 'last', 'count'})
        integer(m['first'], 1); integer(m['last'], m['first']); integer(m['count'], 1)
        if m['count'] != m['last']-m['first']+1:
            raise ValueError('missed count')
    elif r['kind'] == 'terminal':
        keys(r['terminal'], {'status'})
        if r['terminal']['status'] not in {'shutdown_timeout', 'output_unavailable', 'output_limit'}:
            raise ValueError('unknown terminal status')
    else:
        v = r['summary']; keys(v, {'status', 'sources', 'sockets', 'counters', 'problems', 'unobserved'})
        if v['status'] not in {'ok', 'partial'}:
            raise ValueError('round status')
        integer(v['sockets'], hi=256)
        if type(v['unobserved']) is not list:
            raise ValueError('unobserved peer list')
        for p in v['unobserved']:
            keys(p, {'peer', 'role'}); integer(p['peer'], 1, nodes)
            if p['peer'] == node or p['role'] not in {'dialed', 'accepted'}:
                raise ValueError('unobserved peer binding')
        if len({(p['peer'], p['role']) for p in v['unobserved']}) != len(v['unobserved']):
            raise ValueError('duplicate unobserved peer')
        if type(v['problems']) is not list or len(set(v['problems'])) != len(v['problems']) or any(p not in {'history_limit', 'ambiguous_socket'} for p in v['problems']):
            raise ValueError('round problems')
        if type(v['sources']) is not list or [s.get('name') for s in v['sources']] != SOURCES:
            raise ValueError('source set/order')
        for s in v['sources']:
            keys(s, {'name', 'status', 'bytes', 'entries'})
            if s['status'] not in STATUSES:
                raise ValueError('source status')
            integer(s['bytes'], hi=(2 << 20)+1); integer(s['entries'], hi=16385)
        if type(v['counters']) is not list or [c.get('name') for c in v['counters']] != COUNTERS:
            raise ValueError('counter set/order')
        for c in v['counters']:
            keys(c, {'name', 'status', 'value', 'delta_status', 'delta'})
            source = v['sources'][2 if c['name'] != 'TCPTimeouts' else 3]
            if c['status'] != source['status']:
                raise ValueError('counter source mismatch')
            if c['status'] != 'ok':
                if (c['value'], c['delta_status'], c['delta']) != (None, 'unavailable', None):
                    raise ValueError('failed counter has a value')
            else:
                integer(c['value'])
                if c['delta_status'] == 'ok':
                    integer(c['delta'])
                elif c['delta_status'] not in {'initial', 'reset'} or c['delta'] is not None:
                    raise ValueError('delta status')
        partial = bool(v['problems']) or any(s['status'] != 'ok' for s in v['sources'])
        if (v['status'] == 'partial') != partial:
            raise ValueError('inconsistent round status')
    return r


def validate(lines, nodes, duration_ms, interval_ms, node_map=None):
    failures, incomplete = [], []
    rounds, hosts, terminals, shutdowns = defaultdict(list), defaultdict(set), {}, {}
    for i, line in enumerate(lines, 1):
        if 'diag net' not in line:
            continue
        status_match = STATUS_PREFIX.fullmatch(line.rstrip('\n'))
        if status_match:
            try:
                node = int(status_match['node']); integer(node, 1, nodes)
                hosts[node].add(status_match['host'] or '(unprefixed)')
                if node in shutdowns:
                    raise ValueError('duplicate main shutdown status')
                shutdowns[node] = status_match['status']
            except ValueError:
                failures.append(f'line {i}: invalid shutdown status')
            continue
        m = PREFIX.fullmatch(line.rstrip('\n'))
        if not m:
            failures.append(f'line {i}: malformed network record'); continue
        node = int(m['node'])
        try:
            integer(node, 1, nodes)
            record = parse_record(m['data'], node, nodes)
            hosts[node].add(m['host'] or '(unprefixed)')
            if node in terminals:
                raise ValueError('network record after terminal')
            if record['kind'] == 'terminal':
                terminals[node] = record
                incomplete.append(f'node {node}: terminal {record["terminal"]["status"]}')
            else:
                rounds[node].append(record)
        except (ValueError, TypeError, KeyError, AttributeError, OverflowError):
            failures.append(f'line {i}: invalid network schema')
    bound = {}
    if node_map is not None:
        if set(node_map) != set(range(1, nodes+1)) or len(set(node_map.values())) != nodes:
            failures.append('invalid manifest node map')
    for node, hs in hosts.items():
        if len(hs) != 1:
            failures.append(f'node {node}: host changed'); continue
        bound[node] = next(iter(hs))
        if node_map is not None and bound[node] != node_map.get(node):
            failures.append(f'node {node}: host binding mismatch')
    if len(set(bound.values())) != len(bound) and nodes > 1:
        failures.append('several nodes bound to one host or unprefixed stream')
    pairs = defaultdict(set)
    for node in range(1, nodes+1):
        records = rounds[node]
        terminal = terminals.get(node)
        shutdown = shutdowns.get(node)
        if shutdown is None:
            incomplete.append(f'node {node}: missing net-status')
        elif shutdown != 'ok':
            incomplete.append(f'node {node}: net-status {shutdown}')
        if shutdown == 'not_started' and (records or terminal is not None):
            failures.append(f'node {node}: records despite not_started')
        if terminal is not None and shutdown is not None:
            # A worker can encounter an output failure, then block while
            # attempting its terminal marker. The caller can time out later.
            expected_terminal = 'output_unavailable' if shutdown == 'unavailable_sink' else shutdown
            if shutdown != 'shutdown_timeout' and terminal['terminal']['status'] != expected_terminal:
                failures.append(f'node {node}: terminal/net-status mismatch')
        if terminal is not None:
            if records:
                last = records[-1]
                if (terminal['anchor_utc'] != last['anchor_utc'] or
                    terminal['round'] not in {last['round'], last['round']+1} or
                    terminal['start_ms'] < last['end_ms'] or
                    (terminal['round'] == last['round'] and terminal['phase'] != last['phase'])):
                    failures.append(f'node {node}: inconsistent terminal identity/time')
            elif terminal['round'] != 0 or terminal['phase'] != 'initial':
                failures.append(f'node {node}: terminal without capture must be initial round zero')
        if not records:
            incomplete.append(f'node {node}: no rounds'); continue
        groups = []
        for r in records:
            if not groups or r['round'] != groups[-1][0]['round']:
                groups.append([])
            groups[-1].append(r)
        if groups[0][0]['phase'] != 'initial' or groups[-1][0]['phase'] != 'final' or len(groups) < 2:
            incomplete.append(f'node {node}: missing initial/final round')
        anchor, last_end, previous, history = records[0]['anchor_utc'], -1, {}, {}
        for ix, group in enumerate(groups):
            r = group[0]
            if r['round'] != ix or any(any(x[k] != r[k] for k in BASE-{'kind'}) for x in group):
                failures.append(f'node {node}: round identity/order')
            if r['anchor_utc'] != anchor or r['start_ms'] < last_end:
                failures.append(f'node {node}: nonmonotonic read intervals or changed anchor')
            if last_end >= 0 and r['start_ms']-last_end > 1.5*interval_ms:
                incomplete.append(f'node {node}: long read/cadence gap')
            if r['end_ms']-r['start_ms'] > 1.5*interval_ms:
                incomplete.append(f'node {node}: slow round')
            last_end = r['end_ms']
            wanted = 'initial' if ix == 0 else 'final' if ix == len(groups)-1 else 'periodic'
            if missed_slots := [x['missed'] for x in group if x['kind'] == 'missed']:
                if any(m['last'] >= r['start_ms']//interval_ms for m in missed_slots):
                    failures.append(f'node {node}: missed range includes current/future slot')
            if r['phase'] != wanted:
                incomplete.append(f'node {node}: missing initial/final or unexpected phase')
            if ix == 0 and r['start_ms'] > interval_ms:
                incomplete.append(f'node {node}: late initial round')
            if wanted == 'final' and r['end_ms'] < duration_ms:
                incomplete.append(f'node {node}: early final round')
            footers = [x for x in group if x['kind'] == 'round']
            sockets = [x['socket'] for x in group if x['kind'] == 'socket']
            missed = [x for x in group if x['kind'] == 'missed']
            if len(sockets) > 256:
                failures.append(f'node {node}: socket cap exceeded')
            counts = defaultdict(int)
            for s in sockets:
                key = (s['dialer'], s['acceptor'], s['client_port'])
                counts[key] += 1
                if s['ambiguous']:
                    incomplete.append(f'node {node}: ambiguous socket')
                if not s['ambiguous'] and s['generation']:
                    old = history.get(key)
                    if old is not None and (s['generation'] < old or s['generation'] > old+1):
                        failures.append(f'node {node}: inconsistent generation')
                    if old is not None and ((s['lifecycle'] == 'observed' and s['generation'] != old) or
                                            (s['lifecycle'] == 'reappeared_or_replaced' and s['generation'] != old+1) or
                                            s['lifecycle'] == 'first'):
                        failures.append(f'node {node}: lifecycle/generation mismatch')
                    history[key] = s['generation']
                if s['state'] == 1 and not s['ambiguous']:
                    pairs[key].add(node)
            for s in sockets:
                key = (s['dialer'], s['acceptor'], s['client_port'])
                if counts[key] > 1 and not s['ambiguous']:
                    failures.append(f'node {node}: unmarked duplicate socket key')
            if len(footers) == 0 and ix == len(groups)-1 and len(missed) <= 1:
                if terminal is not None:
                    reason = 'terminal interrupted round output'
                elif shutdown == 'ok':
                    reason = 'truncated_despite_ok_status'
                else:
                    reason = 'truncated_without_terminal'
                incomplete.append(f'node {node}: {reason}')
                continue
            if len(footers) != 1 or group[-1]['kind'] != 'round' or len(missed) > 1:
                failures.append(f'node {node}: round footer/missed record'); continue
            v = footers[0]['summary']
            if v['sockets'] != len(sockets):
                failures.append(f'node {node}: socket count')
            if v['status'] != 'ok' or missed:
                incomplete.append(f'node {node}: partial or missed round')
            wanted_peers = {(peer, role) for peer in range(1, nodes+1) if peer != node for role in ('dialed', 'accepted')}
            observed = {(s['peer'], s['role']) for s in sockets if s['peer']}
            if {(p['peer'], p['role']) for p in v['unobserved']} != wanted_peers-observed:
                failures.append(f'node {node}: unobserved peer complement')
            if any(s['ambiguous'] for s in sockets) and 'ambiguous_socket' not in v['problems']:
                failures.append(f'node {node}: missing ambiguity problem')
            for c in v['counters']:
                old = previous.get(c['name'])
                if c['value'] is not None:
                    status = 'initial' if old is None else 'reset' if c['value'] < old else 'ok'
                    delta = c['value']-old if status == 'ok' else None
                    if (c['delta_status'], c['delta']) != (status, delta):
                        failures.append(f'node {node}: invalid counter delta')
                previous[c['name']] = c['value']
    unmatched = [list(k) for k, seen in sorted(pairs.items()) if seen != set(k[:2])]
    if unmatched:
        incomplete.append('established socket keys missing an opposite endpoint somewhere in the capture')
    status = 'invalid' if failures else 'incomplete' if incomplete else 'complete'
    return {'status': status, 'failures': failures, 'incomplete': sorted(set(incomplete)),
            'unmatched_socket_keys': unmatched, 'shutdown_statuses': dict(sorted(shutdowns.items()))}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('run_log'); ap.add_argument('--nodes', type=int, required=True)
    ap.add_argument('--duration-s', type=float, default=30); ap.add_argument('--interval-s', type=float, default=1)
    ap.add_argument('--manifest')
    args = ap.parse_args()
    if args.nodes < 1 or not math.isfinite(args.duration_s) or not math.isfinite(args.interval_s) or args.duration_s <= 0 or args.interval_s <= 0:
        ap.error('positive nodes, duration and interval required')
    try:
        node_map = None
        if args.manifest:
            with open(args.manifest) as f:
                entries = json.load(f)['node_map']
            node_map = {int(x['id']): x['host'] for x in entries}
            if len(node_map) != len(entries):
                raise ValueError('duplicate manifest IDs')
        with open(args.run_log) as f:
            result = validate(f, args.nodes, args.duration_s*1000, args.interval_s*1000, node_map)
    except (OSError, ValueError, KeyError, TypeError):
        result = {'status': 'invalid', 'failures': ['cannot read valid input/manifest']}
    print(json.dumps(result, sort_keys=True))
    return {'complete': 0, 'invalid': 1, 'incomplete': 4}[result['status']]


if __name__ == '__main__':
    sys.exit(main())
