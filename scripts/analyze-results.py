#!/usr/bin/env python3

'''
Analyze the results.
The input to this script should be the output from parse-results.py, ex:
```
./scripts/parse-results.py > results.txt
./scripts/analyze-results.py AGGREGATE results.txt
```
'''

import math
import os
import sqlite3
import sys

GROUP_COLS = [
    'backend',
    'example',
    'experiment',
]

GROUP_COLS_STR = lambda prefix: ', '.join([prefix + '.' + col for col in GROUP_COLS])

# Enfirce a specific ordering on the examples.
EXAMPLE_RANK_IDS = {
    'stance-createdebate': '01-stance-createdebate',
    'simple-acquaintances': '02-simple-acquaintances',
    'stance-4forums': '03-stance-4forums',
    'trust-prediction': '04-trust-prediction',
    'friendship': '05-friendship',
    'epinions': '06-epinions',
    'citeseer': '07-citeseer',
    'cora': '08-cora',
    'user-modeling': '09-user-modeling',
    'knowledge-graph-identification': '10-knowledge-graph-identification',
    'yelp': '11-yelp',
    'social-network-analysis': '12-social-network-analysis',
    'entity-resolution': '13-entity-resolution',
    'jester': '14-jester',
    'lastfm': '15-lastfm',
}

EXAMPLE_RANK_IDS_QUERY = " UNION ALL ".join(["SELECT '%s' AS example, '%s' AS id" % (key, value) for (key, value) in EXAMPLE_RANK_IDS.items()])

# Get all results with an actual value (e.g. ignore incomplete runs).
BASE_QUERY = '''
    SELECT *
    FROM Stats S
    WHERE S.runtime IS NOT NULL
    ORDER BY
        ''' + GROUP_COLS_STR('S') + '''
'''

# Aggregate over splits/iterations.
AGGREGATE_QUERY = '''
    SELECT
        I.id,
        ''' + GROUP_COLS_STR('S') + ''',
        COUNT(S.split) AS numIterations,
        AVG(S.groundrules) AS groundrules,
        AVG(S.memory) AS memory_mean,
        STDEV(S.memory) AS memory_std,
        AVG(S.runtime) AS runtime_mean,
        STDEV(S.runtime) AS runtime_std,
        AVG(S.grounding_time) AS grounding_time_mean,
        STDEV(S.grounding_time) AS grounding_time_std
    FROM
        (
            ''' + BASE_QUERY + '''
        ) S
        JOIN (
            ''' + EXAMPLE_RANK_IDS_QUERY + '''
        ) I ON I.example = S.example
    GROUP BY
        ''' + GROUP_COLS_STR('S') + '''
    ORDER BY
        I.id,
        S.example,
        S.backend,
        ''' + GROUP_COLS_STR('S') + '''
'''

# Pose results as relative to postgres on the same split/iteration.
RELATIVE_QUERY = '''
    SELECT
        ''' + GROUP_COLS_STR('S1') + ''',
        S1.split,
        S1.iteration,
        S1.groundrules,
        S1.memory / CAST(S2.memory AS REAL) AS relative_memory,
        S1.runtime / CAST(S2.runtime AS REAL) AS relative_runtime,
        S1.grounding_time / CAST(S2.grounding_time AS REAL) AS relative_grounding_time
    FROM
        (
            ''' + BASE_QUERY + '''
        ) S1
        JOIN (
            ''' + BASE_QUERY + '''
        ) S2 USING (example, experiment, split, iteration)
    WHERE
        S2.backend = 'Postgres'
    ORDER BY
        ''' + GROUP_COLS_STR('S1') + ''',
        S1.split,
        S1.iteration
'''

# Aggregate over the relative query.
RELATIVE_AGGREGATE_QUERY = '''
    SELECT
        I.id,
        ''' + GROUP_COLS_STR('S') + ''',
        COUNT(S.split) AS numIterations,
        AVG(S.groundrules) AS groundrules,
        AVG(S.relative_memory) AS relative_memory_mean,
        STDEV(S.relative_memory) AS relative_memory_std,
        AVG(S.relative_runtime) AS relative_runtime_mean,
        STDEV(S.relative_runtime) AS relative_runtime_std,
        AVG(S.relative_grounding_time) AS relative_grounding_time_mean,
        STDEV(S.relative_grounding_time) AS relative_grounding_time_std
    FROM
        (
            ''' + RELATIVE_QUERY + '''
        ) S
        JOIN (
            ''' + EXAMPLE_RANK_IDS_QUERY + '''
        ) I ON I.example = S.example
    GROUP BY
        I.id,
        ''' + GROUP_COLS_STR('S') + '''
    ORDER BY
        I.id,
        ''' + GROUP_COLS_STR('S') + '''
'''

BOOL_COLUMNS = {
}

INT_COLUMNS = {
    'iteration',
    'groundrules',
    'memory',
    'runtime',
    'grounding_time',
}

FLOAT_COLUMNS = {
}

# {key: (query, description), ...}
RUN_MODES = {
    'BASE': (
        BASE_QUERY,
        'Just get the results with no additional processing.',
    ),
    'AGGREGATE': (
        AGGREGATE_QUERY,
        'Aggregate over iteration.',
    ),
    'RELATIVE': (
        RELATIVE_QUERY,
        'Pose results as relative to postgres on the same split/iteration.',
    ),
    'RELATIVE_AGGREGATE': (
        RELATIVE_AGGREGATE_QUERY,
        'Aggregate over the relative query.',
    ),
}

# ([header, ...], [[value, ...], ...])
def fetchResults(path):
    rows = []
    header = None

    with open(path, 'r') as file:
        for line in file:
            line = line.strip("\n ")
            if (line == ''):
                continue

            row = line.split("\t")

            # Get the header first.
            if (header is None):
                header = row
                continue

            assert(len(header) == len(row))

            for i in range(len(row)):
                if (row[i] == ''):
                    row[i] = None
                elif (header[i] in BOOL_COLUMNS):
                    row[i] = (row[i].upper() == 'TRUE')
                elif (header[i] in INT_COLUMNS):
                    row[i] = int(row[i])
                elif (header[i] in FLOAT_COLUMNS):
                    row[i] = float(row[i])

            rows.append(row)

    return header, rows

# Standard deviation UDF for sqlite3.
# Taken from: https://www.alexforencich.com/wiki/en/scripts/python/stdev
class StdevFunc:
    def __init__(self):
        self.M = 0.0
        self.S = 0.0
        self.k = 1

    def step(self, value):
        if value is None:
            return
        tM = self.M
        self.M += (value - tM) / self.k
        self.S += (value - tM) * (value - self.M)
        self.k += 1

    def finalize(self):
        if self.k < 3:
            return None
        return math.sqrt(self.S / (self.k-2))

def main(mode, resultsPath):
    columns, data = fetchResults(resultsPath)
    if (len(data) == 0):
        return

    quotedColumns = ["'%s'" % column for column in columns]

    columnDefs = []
    for i in range(len(columns)):
        column = columns[i]
        quotedColumn = quotedColumns[i]

        if (column in BOOL_COLUMNS):
            columnDefs.append("%s INTEGER" % (quotedColumn))
        elif (column in INT_COLUMNS):
            columnDefs.append("%s INTEGER" % (quotedColumn))
        elif (column in FLOAT_COLUMNS):
            columnDefs.append("%s FLOAT" % (quotedColumn))
        else:
            columnDefs.append("%s TEXT" % (quotedColumn))

    connection = sqlite3.connect(":memory:")
    connection.create_aggregate("STDEV", 1, StdevFunc)

    connection.execute("CREATE TABLE Stats(%s)" % (', '.join(columnDefs)))

    connection.executemany("INSERT INTO Stats(%s) VALUES (%s)" % (', '.join(columns), ', '.join(['?'] * len(columns))), data)

    query = RUN_MODES[mode][0]
    rows = connection.execute(query)

    print("\t".join([column[0] for column in rows.description]))
    for row in rows:
        print("\t".join(map(str, row)))

    connection.close()

def _load_args(args):
    executable = args.pop(0)
    if (len(args) != 2 or ({'h', 'help'} & {arg.lower().strip().replace('-', '') for arg in args})):
        print("USAGE: python3 %s <results path> <mode>" % (executable), file = sys.stderr)
        print("modes:", file = sys.stderr)
        for (key, (query, description)) in RUN_MODES.items():
            print("    %s - %s" % (key, description), file = sys.stderr)
        sys.exit(1)

    resultsPath = args.pop(0)
    if (not os.path.isfile(resultsPath)):
        raise ValueError("Can't find the specified results path: " + resultsPath)

    mode = args.pop(0).upper()
    if (mode not in RUN_MODES):
        raise ValueError("Unknown mode: '%s'." % (mode))

    return mode, resultsPath

if (__name__ == '__main__'):
    main(*_load_args(sys.argv))
