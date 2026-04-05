ATOMIC_CRATE := ../../crates/atomic-python

.PHONY: install run-local run-sum

install:
	cd $(ATOMIC_CRATE) && maturin develop --release

run-local: install
	python local_word_count.py

run-sum: install
	python local_sum_job.py
