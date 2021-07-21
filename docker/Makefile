IMG_ORG ?= oasisprotocol
IMG_VER ?= master

IMAGES = oasis-core-dev oasis-core-ci oasis-core-build

.PHONY: all $(IMAGES)

all: $(IMAGES)

oasis-core-dev: development/Dockerfile
	docker build development -t $(IMG_ORG)/$@:$(IMG_VER)

oasis-core-ci: testing/Dockerfile development/Dockerfile
	docker build --build-arg OASIS_CORE_DEV_BASE_TAG=$(IMG_VER) testing -t $(IMG_ORG)/$@:$(IMG_VER)

oasis-core-build: build/Dockerfile
	docker build build -t $(IMG_ORG)/$@:$(IMG_VER)

# Generates and copies the tarball in current directory.
reproducible: oasis-core-build
	docker run -e GIT_TAG=$(IMG_VER) --name oasis-reproducible -ti $(IMG_ORG)/oasis-core-build:$(IMG_VER) bash -c "`cat build/reproducible.sh`"
	docker cp oasis-reproducible:/root/oasis-core/oasis_core_reproducible_${IMG_VER}_linux_amd64.tar.gz .
	docker rm oasis-reproducible
