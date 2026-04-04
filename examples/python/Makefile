ATOMIC_CRATE := ../../crates/atomic-python

.PHONY: install run-local run-docker

install:
	cd $(ATOMIC_CRATE) && maturin develop --release

run-local: install
	python local_word_count.py

run-docker: install
	python docker_sum_job.py
