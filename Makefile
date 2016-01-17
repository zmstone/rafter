PROJECT = gen_raft
PROJECT_DESCRIPTION = High level raft abstraction
PROJECT_VERSION = 0.0.1

COVER = true

include erlang.mk

ERLC_OPTS += -DAPPLICATION=gen_raft
TEST_ERLC_OPTS += -DAPPLICATION=gen_raft

cover-print:
	./scripts/cover-print-not-covered-lines.escript ./ct.coverdata ./eunit.coverdata
	./scripts/cover-print-summary.escript ./ct.coverdata ./eunit.coverdata

t: ct cover-print

