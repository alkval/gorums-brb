import copy
import importlib.util
import json
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('netvalidator', Path(__file__).with_name('validate-diag-net.py'))
v = importlib.util.module_from_spec(spec)
spec.loader.exec_module(v)


def socket(node):
    return dict(dialer=1, acceptor=2, client_port=45001, peer=3-node,
                role='dialed' if node == 1 else 'accepted', state=1,
                tx_queue=12, rx_queue=34, timer_active=1, timer_ticks=100,
                unrecovered_rto=2, zero_window_probes=0, generation=1,
                lifecycle='first', reconnect=False, ambiguous=False, field_status='ok')


def record(ix, kind, node):
    r = dict(version=1, round=ix, kind=kind, phase=['initial', 'periodic', 'final'][ix],
             start_ms=ix*1000, end_ms=ix*1000+1, anchor_utc='2026-10-29T12:00:00Z')
    if kind == 'socket':
        r['socket'] = socket(node)
        if ix:
            r['socket']['lifecycle'] = 'observed'
    else:
        r['summary'] = dict(status='ok', sockets=1, problems=[],
            unobserved=[dict(peer=3-node, role='accepted' if node == 1 else 'dialed')],
            sources=[dict(name=name, status='ok', bytes=100, entries=1 if i < 2 else 0)
                     for i, name in enumerate(v.SOURCES)],
            counters=[dict(name=name, status='ok', value=10+ix,
                           delta_status='initial' if ix == 0 else 'ok', delta=None if ix == 0 else 1)
                      for name in v.COUNTERS])
    return r


def fixture():
    return [(n, record(i, k, n)) for i in range(3) for n in (1, 2) for k in ('socket', 'round')]


def lines(records, statuses=None):
    log = [f'2026/10/29 12:00:00 [host{n}:9000] brb bench: node {n} diag net {json.dumps(r)}\n' for n, r in records]
    if statuses is None:
        statuses = {n: 'ok' for n, _ in records}
        for n, r in records:
            if r['kind'] == 'terminal':
                statuses[n] = r['terminal']['status']
    for n, status in statuses.items():
        log.append(f'2026/10/29 12:00:00 [host{n}:9000] brb bench: node {n} diag net-status status={status}\n')
    return log


def validate(records):
    return v.validate(lines(records), 2, 2000, 1000, {1: 'host1:9000', 2: 'host2:9000'})


class NetValidatorTests(unittest.TestCase):
    def test_go_fixture(self):
        path = Path(__file__).with_name('testdata') / 'diag-net-v1.log'
        result = v.validate(path.read_text().splitlines(), 2, 2000, 1000,
                            {1: 'host1:9000', 2: 'host2:9000'})
        self.assertEqual(result['status'], 'complete', result)

    def test_go_terminal_fixture(self):
        path = Path(__file__).with_name('testdata') / 'diag-net-terminal-v1.log'
        result = v.validate(path.read_text().splitlines(), 2, 2000, 1000,
                            {1: 'host1:9000', 2: 'host2:9000'})
        self.assertEqual(result['status'], 'incomplete', result)
        self.assertEqual(result['failures'], [])
        self.assertTrue(any('terminal interrupted' in x for x in result['incomplete']))

    def test_only_initial_is_incomplete(self):
        self.assertEqual(validate(fixture()[:4])['status'], 'incomplete')

    def test_complete(self):
        self.assertEqual(validate(fixture())['status'], 'complete')

    def test_strict_schema(self):
        for change in [lambda r: r.update(address='private'), lambda r: r.update(round=True),
                       lambda r: r.update(start_ms=-1), lambda r: r.update(version=2),
                       lambda r: r.update(anchor_utc='no'), lambda r: r.update(socket={}),
                       lambda r: r['socket'].update(tx_queue=None),
                       lambda r: r['socket'].update(peer=1),
                       lambda r: r['socket'].update(state=6),
                       lambda r: r['socket'].update(reconnect=1),
                       lambda r: r['socket'].update(lifecycle='first', generation=2)]:
            with self.subTest(change=change):
                records = fixture(); change(records[0][1])
                self.assertEqual(validate(records)['status'], 'invalid')

    def test_duplicate_json_keys_and_nonfinite(self):
        for raw in ['{"version":1,"version":1}', '{"kind":"socket","round":NaN}']:
            with self.assertRaises(ValueError):
                v.parse_record(raw, 1, 2)

    def test_missing_round_footer_and_socket_count(self):
        self.assertEqual(validate(fixture()[1:])['status'], 'invalid')
        self.assertEqual(validate(fixture()[:-1])['status'], 'incomplete')
        r = fixture(); r[1][1]['summary']['sockets'] = 2
        self.assertEqual(validate(r)['status'], 'invalid')

    def test_missing_node_and_final(self):
        for r in [[(n, x) for n, x in fixture() if n == 1], fixture()[:-4]]:
            self.assertEqual(validate(r)['status'], 'incomplete')

    def test_host_binding(self):
        log = lines(fixture()); log[0] = log[0].replace('host1', 'host2')
        self.assertEqual(v.validate(log, 2, 2000, 1000)['status'], 'invalid')
        self.assertEqual(v.validate(lines(fixture()), 2, 2000, 1000, {1: 'other', 2: 'host2:9000'})['status'], 'invalid')
        log = lines(fixture()); log[0] = log[0].replace('node 1', 'node 99')
        self.assertEqual(v.validate(log, 2, 2000, 1000)['status'], 'invalid')

    def test_failed_source_is_null(self):
        r = fixture()
        for n, x in r:
            if n == 1 and x['round'] == 1 and x['kind'] == 'round':
                x['summary']['sources'][2]['status'] = 'read_error'
                x['summary']['status'] = 'partial'
                for c in x['summary']['counters'][:2]:
                    c.update(status='read_error', value=None, delta_status='unavailable', delta=None)
            if n == 1 and x['round'] == 2 and x['kind'] == 'round':
                for c in x['summary']['counters'][:2]:
                    c.update(delta_status='initial', delta=None)
        self.assertEqual(validate(r)['status'], 'incomplete')
        r[5][1]['summary']['counters'][0]['value'] = 0
        self.assertEqual(validate(r)['status'], 'invalid')

    def test_reset(self):
        r = fixture()
        for n, x in r:
            if n == 1 and x['round'] == 2 and x['kind'] == 'round':
                for c in x['summary']['counters']:
                    c.update(value=1, delta_status='reset', delta=None)
        self.assertEqual(validate(r)['status'], 'complete')
        r[-3][1]['summary']['counters'][0]['delta_status'] = 'ok'
        self.assertEqual(validate(r)['status'], 'invalid')

    def test_unmarked_duplicates(self):
        r = fixture(); r.insert(1, copy.deepcopy(r[0])); r[2][1]['summary']['sockets'] = 2
        self.assertEqual(validate(r)['status'], 'invalid')
        for _, x in r[:2]:
            x['socket']['ambiguous'] = True
        r[2][1]['summary'].update(status='partial', problems=['ambiguous_socket'])
        self.assertEqual(validate(r)['status'], 'incomplete')

    def test_missed_and_slow_rounds(self):
        r = fixture()
        for node, x in r:
            if node == 1 and x['round'] > 0:
                x['start_ms'] += 1000; x['end_ms'] += 1000
        missed = copy.deepcopy(r[4][1]); missed.pop('socket')
        missed.update(kind='missed', missed=dict(first=1, last=1, count=1))
        r.insert(4, (1, missed))
        self.assertEqual(validate(r)['status'], 'incomplete')
        r[4][1]['missed']['count'] = 2
        self.assertEqual(validate(r)['status'], 'invalid')
        r = fixture()
        for _, x in r[4:8]:
            x['end_ms'] = 2900
        self.assertEqual(validate(r)['status'], 'invalid')  # overlaps next round

    def test_unmatched_pair(self):
        r = fixture()
        for n, x in r:
            if n == 2 and x['kind'] == 'socket':
                x['socket']['client_port'] = 45002
        result = validate(r)
        self.assertEqual(result['status'], 'incomplete')
        self.assertEqual(len(result['unmatched_socket_keys']), 2)

    def test_time_wait_null(self):
        r = fixture()
        for _, x in r:
            if x['kind'] == 'socket':
                x['socket'].update(state=6, field_status='state_only', lifecycle='unknown', generation=0)
                x['socket'].update({k: None for k in v.QUEUES})
        self.assertEqual(validate(r)['status'], 'complete')

    def test_trailing_without_terminal_is_incomplete(self):
        interleaved = [(n, x) for n, x in fixture() if n != 1 or x['round'] == 0 or
                       (x['round'] == 1 and x['kind'] == 'socket')]
        for records in (fixture()[:5], fixture()[:-1], interleaved):
            result = validate(records)
            self.assertEqual(result['status'], 'incomplete', result)
            self.assertEqual(result['failures'], [])
            self.assertTrue(any('truncated_despite_ok_status' in x for x in result['incomplete']))

    def test_trailing_truncation_labels(self):
        for status, label in [('ok', 'truncated_despite_ok_status'),
                              ('shutdown_timeout', 'truncated_without_terminal'),
                              (None, 'truncated_without_terminal')]:
            statuses = {2: 'ok'}
            if status is not None:
                statuses[1] = status
            result = v.validate(lines(fixture()[:5], statuses), 2, 2000, 1000)
            self.assertEqual(result['status'], 'incomplete', result)
            self.assertEqual(result['failures'], [])
            self.assertIn(f'node 1: {label}', result['incomplete'])

    def test_not_started_status(self):
        result = v.validate(lines([], {1: 'not_started', 2: 'not_started'}), 2, 2000, 1000)
        self.assertEqual(result['status'], 'incomplete', result)
        self.assertEqual(result['failures'], [])
        self.assertEqual(result['shutdown_statuses'], {1: 'not_started', 2: 'not_started'})
        result = v.validate(lines(fixture(), {1: 'not_started', 2: 'ok'}), 2, 2000, 1000)
        self.assertEqual(result['status'], 'invalid', result)
        self.assertTrue(any('records despite not_started' in x for x in result['failures']))

    def test_unfinished_round_with_later_round_is_invalid(self):
        r = fixture()
        r.pop(5)  # node 1 periodic footer, with its final round still following
        self.assertEqual(validate(r)['status'], 'invalid')

    def test_trailing_without_terminal_stays_strict(self):
        for mode in ('duplicate', 'generation', 'schema', 'cap'):
            r = fixture()[:5]
            if mode == 'duplicate':
                r.append(copy.deepcopy(r[-1]))
            elif mode == 'generation':
                r[-1][1]['socket'].update(generation=2, lifecycle='observed')
            elif mode == 'schema':
                r[-1][1]['socket']['tx_queue'] = -1
            else:
                base = copy.deepcopy(r[-1])
                for port in range(45002, 45002+256):
                    extra = copy.deepcopy(base); extra[1]['socket']['client_port'] = port
                    r.append(extra)
            self.assertEqual(validate(r)['status'], 'invalid', mode)

    def test_main_status_is_strict_and_bound(self):
        baseline = lines(fixture())
        for line in (baseline[-1], baseline[-1].replace('status=ok', 'status=bogus'),
                     baseline[-1].replace('node 2', 'node 99'),
                     baseline[-1].replace('status=ok', 'status=ok extra=1'),
                     baseline[-1].replace('host2', 'other')):
            self.assertEqual(v.validate(baseline+[line], 2, 2000, 1000)['status'], 'invalid')
        result = v.validate(lines(fixture(), statuses={}), 2, 2000, 1000)
        self.assertEqual(result['status'], 'incomplete')
        self.assertTrue(any('missing net-status' in x for x in result['incomplete']))

    def test_main_status_survives_missing_terminal(self):
        for status in ('shutdown_timeout', 'output_unavailable', 'output_limit', 'unavailable_sink'):
            log = lines(fixture()[:5], statuses={1: status, 2: 'ok'})
            result = v.validate(log, 2, 2000, 1000)
            self.assertEqual(result['status'], 'incomplete', result)
            self.assertEqual(result['failures'], [])
            self.assertEqual(result['shutdown_statuses'][1], status)

    def test_main_status_terminal_crosscheck_and_late_marker(self):
        r = fixture()
        term = record(2, 'round', 1); term.pop('summary')
        term.update(kind='terminal', terminal={'status': 'output_unavailable'}, start_ms=2100, end_ms=2100)
        r.append((1, term))
        for status in ('output_unavailable', 'unavailable_sink', 'shutdown_timeout'):
            result = v.validate(lines(r, {1: status, 2: 'ok'}), 2, 2000, 1000)
            self.assertEqual(result['status'], 'incomplete', result)
            self.assertEqual(result['failures'], [])
        for status in ('ok', 'output_limit'):
            self.assertEqual(v.validate(lines(r, {1: status, 2: 'ok'}), 2, 2000, 1000)['status'], 'invalid')
        # The timed-out worker can publish its marker after the main line.
        log = lines(fixture(), {1: 'shutdown_timeout', 2: 'ok'})
        log += lines([(1, term)], statuses={})
        self.assertEqual(v.validate(log, 2, 2000, 1000)['status'], 'incomplete')

    def test_timeout_after_complete_final_footer_is_conservative(self):
        r = fixture(); term = record(2, 'round', 1); term.pop('summary')
        term.update(kind='terminal', terminal={'status': 'shutdown_timeout'}, start_ms=2100, end_ms=2100)
        r.append((1, term))
        result = validate(r)
        self.assertEqual(result['status'], 'incomplete', result)
        self.assertEqual(result['failures'], [])

    def test_terminal_failure_is_incomplete(self):
        for reason in ('output_unavailable', 'output_limit', 'shutdown_timeout'):
            r = fixture()[:5]  # one socket in the interrupted second round
            term = copy.deepcopy(r[-1][1]); term.pop('socket')
            term.update(kind='terminal', terminal={'status': reason})
            term['start_ms'] = term['end_ms'] = 1100
            r.append((1, term))
            result = validate(r)
            self.assertEqual(result['status'], 'incomplete', result)
            self.assertTrue(any('terminal interrupted' in x for x in result['incomplete']))
        # A sink can fail before any socket/footer was written.
        term = record(0, 'round', 1); term.pop('summary')
        term.update(kind='terminal', terminal={'status': 'output_unavailable'})
        self.assertEqual(validate([(1, term)])['status'], 'incomplete')

    def test_terminal_does_not_waive_socket_consistency(self):
        for mode in ('duplicate', 'generation'):
            r = fixture()[:5]
            term = copy.deepcopy(r[-1][1]); term.pop('socket')
            term.update(kind='terminal', terminal={'status': 'output_unavailable'}, start_ms=1100, end_ms=1100)
            if mode == 'duplicate':
                r.append(copy.deepcopy(r[-1]))
            else:
                r[-1][1]['socket'].update(generation=2, lifecycle='observed')
            r.append((1, term))
            self.assertEqual(validate(r)['status'], 'invalid')

    def test_terminal_remains_strict(self):
        for change in (lambda t: t['terminal'].update(status='bogus'),
                       lambda t: t['terminal'].update(path='private'),
                       lambda t: t.update(round=99),
                       lambda t: t.update(start_ms=0, end_ms=0),
                       lambda t: t.update(anchor_utc='2026-10-30T12:00:00Z')):
            r = fixture(); term = record(2, 'round', 1); term.pop('summary')
            term.update(kind='terminal', terminal={'status': 'shutdown_timeout'}, start_ms=2100, end_ms=2100)
            change(term); r.append((1, term))
            self.assertEqual(validate(r)['status'], 'invalid')
        r = fixture(); term = record(2, 'round', 1); term.pop('summary')
        term.update(kind='terminal', terminal={'status': 'shutdown_timeout'}, start_ms=2100, end_ms=2100)
        r.extend([(1, term), (1, term)])
        self.assertEqual(validate(r)['status'], 'invalid')
        r = fixture(); r.insert(0, (1, term))
        self.assertEqual(validate(r)['status'], 'invalid')

    def test_missed_cannot_include_current_slot(self):
        r = fixture(); missed = copy.deepcopy(r[8][1]); missed.pop('socket')
        missed.update(kind='missed', missed={'first': 2, 'last': 2, 'count': 1})
        r.insert(8, (1, missed))
        self.assertEqual(validate(r)['status'], 'invalid')

    def test_bad_counter_and_unobserved_list(self):
        for mutate in [lambda s: s['counters'][0].update(delta=99),
                       lambda s: s.update(unobserved=[]),
                       lambda s: s['sources'][0].update(status='unexpected'),
                       lambda s: s.update(status='partial'),
                       lambda s: s.update(sources=[]), lambda s: s.update(counters=[])]:
            r = fixture(); mutate(r[5][1]['summary'])
            self.assertEqual(validate(r)['status'], 'invalid')


if __name__ == '__main__':
    unittest.main()
