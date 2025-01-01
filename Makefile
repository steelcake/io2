run_fuzz:
	rm -rf data && mkdir data && cargo fuzz run fuzz_target_1
