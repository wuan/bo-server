# Makefile of FunkLoad xmlrpc demo
.PHONY: clean all start stop restart status
.PHONY:	test blitzortung_test bench blitzortung_bench
.PHONY:	start_monitor stop_monitor restart_monitor
.PHONY:	start_blitzortung stop_blitzortung restart_blitzortung

BOCTL := fl-blitzortung-ctl cred.conf
MONCTL := fl-monitor-ctl monitor.conf

ifdef URL
# FunkLoad options
	FLOPS = -u $(URL)
else
	FLOPS =
endif

all: test

# testing
test: blitzortung_test

blitzortung_test:
	$(BOCTL) restart
	fl-run-test test_Credential.py -v $(FLOPS)
	$(BOCTL) stop

# benching
bench: blitzortung_bench

blitzortung_bench:
	@$(BOCTL) restart
	@$(MONCTL) restart
	-fl-run-bench -c 1:20:40:60:80:100 -D 10 test_Credential.py Credential.test_blitzortung $(FLOPS)
	-fl-build-report blitzortung-bench.xml --html
	@$(MONCTL) stop
	@$(BOCTL) stop


# monitor ctl
start_monitor:
	$(MONCTL) start

stop_monitor:
	-$(MONCTL) stop

restart_monitor:
	$(MONCTL) restart

# blitzortung ctl
start_blitzortung:
	$(BOCTL) start

stop_blitzortung:
	$(BOCTL) stop

restart_blitzortung:
	$(BOCTL) restart

# misc
status:
	$(MONCTL) status;
	$(BOCTL) status;

stop: stop_monitor stop_blitzortung

start: start_monitor start_blitzortung

restart: restart_monitor restart_blitzortung

clean:
	-find . "(" -name "*~" -or  -name ".#*" ")" -print0 | xargs -0 rm -f
