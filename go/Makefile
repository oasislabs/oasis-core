include ../common.mk

# Check if Go's linkers flags are set in common.mk and add them as extra flags.
ifneq ($(GOLDFLAGS),)
	GO_EXTRA_FLAGS += -ldflags $(GOLDFLAGS)
endif

# Set all target as the default target.
all: build

# Generate.
generate:
	@$(ECHO) "$(MAGENTA)*** Running go generate...$(OFF)"
	@$(GO) generate ./...
	@$(MAKE) fmt

# Build.
# List of Go binaries to build.
go-binaries := oasis-node oasis-test-runner oasis-net-runner oasis-remote-signer \
	oasis-test-runner/scenario/pluginsigner/example_signer_plugin \
	extra/extract-metrics extra/conbench

$(go-binaries):
	@$(ECHO) "$(MAGENTA)*** Building $@...$(OFF)"
	@$(GO) build $(GOFLAGS) $(GO_EXTRA_FLAGS) -o ./$@/$(notdir $@) ./$@
ifeq ($(GO_BUILD_E2E_COVERAGE),1)
	@$(ECHO) "$(MAGENTA)*** Building $@ with E2E coverage...$(OFF)"
	@$(GO) test $(GOFLAGS) -c -tags e2ecoverage -covermode=atomic -coverpkg=./... -o ./$@/$(notdir $@).test ./$@
endif

build: $(go-binaries)

# Build test helpers.
# List of test helpers to build.
test-helpers := mkvs

# MKVS interoperability test helpers.
mkvs:
	@$(ECHO) "$(MAGENTA)*** Building test helpers for $@...$(OFF)"
	@$(GO) build $(GOFLAGS) $(GO_EXTRA_FLAGS) -o ./$(GO_TEST_HELPER_MKVS_PATH) ./$(shell dirname $(GO_TEST_HELPER_MKVS_PATH))

build-helpers: $(test-helpers)

# List of test vectors to generate.
test-vectors-targets := staking/gen_vectors \
	registry/gen_vectors

$(test-vectors-targets):
	@$(ECHO) "$(MAGENTA)*** Generating test vectors ($@)...$(OFF)"
	@$(GO) run ./$@

# Format code.
fmt:
	@$(ECHO) "$(CYAN)*** Running Go formatters...$(OFF)"
	@gofumpt -s -w .
	@gofumports -w -local github.com/oasisprotocol/oasis-core .

# Lint.
lint:
	@$(ECHO) "$(CYAN)*** Running Go linters...$(OFF)"
	@env -u GOPATH golangci-lint run --timeout 4m

# Test.
test-targets := test-unit test-node

test-unit:
	@$(ECHO) "$(CYAN)*** Running Go unit tests...$(OFF)"
	@$(GO) test -timeout 5m -race -v $(GO_TEST_FLAGS) \
	  $$($(GO) list ./... | grep --invert-match github.com/oasisprotocol/oasis-core/go/oasis-node)

test-node:
	@$(ECHO) "$(CYAN)*** Running Go node tests...$(OFF)"
	@$(GO) test -timeout 5m -race -v $(GO_TEST_FLAGS) github.com/oasisprotocol/oasis-core/go/oasis-node/...

test: $(test-targets)

# Test without caching.
force-test:
	@$(ECHO) "$(CYAN)*** Running Go unit tests in force mode...$(OFF)"
	@$(MAKE) test GO_TEST_FLAGS=-count=1

# Fuzzing.
fuzz-targets := fuzz-consensus \
	fuzz-storage \
	fuzz-mkvs/Tree \
	fuzz-mkvs/Proof \
	fuzz-mkvs/Node

define canned-fuzz-run
@TARGETDIR=$(shell pwd)/$<; \
WORKDIR=/tmp/oasis-node-$@; \
if [ "$(FUZZ_NO_BUILD)" != "1" ]; then \
	mkdir -p "$$WORKDIR"; \
	pushd $$TARGETDIR >/dev/null; \
		$(ECHO) "$(CYAN)*** Building fuzzer for $@...$(OFF)"; \
		go-fuzz-build -o $$WORKDIR/fuzz.zip || exit 1; \
	popd >/dev/null; \
fi; \
if [ "$(FUZZ_BUILD_ONLY)" != "1" ]; then \
	mkdir -p "$$WORKDIR"; \
	cd "$$WORKDIR"; \
	$(ECHO) "$(CYAN)*** Running fuzzer for $@...$(OFF)"; \
	if [ "$(@D)" == "." ]; then \
		go-fuzz -bin=$$WORKDIR/fuzz.zip -workdir=$$WORKDIR || exit 1; \
	else \
		go-fuzz -bin=$$WORKDIR/fuzz.zip -workdir=$$WORKDIR -func Fuzz$(@F) || exit 1; \
	fi; \
fi;
endef

# Fuzz consensus transactions.
fuzz-consensus: consensus/tendermint/fuzz/
	$(canned-fuzz-run)
# Fuzz general storage interface.
fuzz-storage: storage/fuzz/ oasis-node
	@mkdir -p /tmp/oasis-node-fuzz-storage/identity
	@chmod 0700 /tmp/oasis-node-fuzz-storage/identity
	@oasis-node/oasis-node identity init --datadir /tmp/oasis-node-fuzz-storage/identity
	$(canned-fuzz-run)
# Fuzz MKVS data structures.
fuzz-mkvs/Tree: storage/mkvs/fuzz
	$(canned-fuzz-run)
fuzz-mkvs/Proof: storage/mkvs/fuzz
	$(canned-fuzz-run)
fuzz-mkvs/Node: storage/mkvs/fuzz
	$(canned-fuzz-run)

# Target that only builds all fuzzing infrastructure.
build-fuzz: FUZZ_BUILD_ONLY=1
build-fuzz: $(fuzz-targets)

# Clean.
clean:
	@$(ECHO) "$(CYAN)*** Cleaning up Go...$(OFF)"
	@$(GO) clean

# List of targets that are not actual files.
.PHONY: \
	generate $(go-binaries) $(go-plugins) build \
	$(test-helpers) build-helpers \
	$(test-vectors-targets) \
	fmt lint \
	$(test-targets) test force-test \
	$(fuzz-targets) build-fuzz \
	clean all

.FORCE:
