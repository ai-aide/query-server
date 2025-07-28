.PHONY: fmt
fmt:
	cargo check && autocorrect --fix && cargo fmt