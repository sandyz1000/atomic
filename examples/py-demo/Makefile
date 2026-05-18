ATOMIC_CRATE := ../../crates/atomic-py

.PHONY: install run-wordcount run-sum run-distributed run

install:
	cd $(ATOMIC_CRATE) && maturin develop --release

run-wordcount: install
	python3 src/local_word_count.py

run-sum: install
	python3 src/local_sum_job.py

run-distributed: install
	RUST_LOG=info python3 src/distributed_word_count.py

run: install
	python3 main.py
