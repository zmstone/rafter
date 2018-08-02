all: compile

.PHONY: t
t: eunit cover

.PHONY: clean
clean:
	@rebar3 clean

.PHONY: eunit
eunit:
	@rebar3 eunit -v --sname raft

.PHONY: compile
compile:
	@rebar3 compile

.PHONY: distclean
distclean: clean
	@rm -rf _build deps
	@rm -f rebar.lock

.PHONY: edoc
edoc:
	@rebar3 edoc

.PHONY: dialyzer
dialyzer:
	@rebar3 dialyzer

.PHONY: hex-publish
hex-publish: distclean
	@rebar3 hex publish

.PHONY: cover
cover:
	@rebar3 cover -v

.PHONY: es
es: compile
	@rebar3 escriptize

.PHONY: rel
rel:
	@rebar3 release
