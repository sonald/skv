all: build

.PHONY: build proto
build: go.mod skvcli skv

skvcli: go.mod
	go build -o out/$@ github.com/sonald/skv/cmd/skvcli

skv: go.mod
	go build -o out/$@ github.com/sonald/skv/cmd/skv

proto: internal/pkg/rpc/service.proto
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative internal/pkg/rpc/service.proto


.PHONY: test
# testing example
test: skv skvcli
	./skv -B --raft.id server-1 --raft.bind 127.0.0.1:9527 -r /tmp/skv
	./skv --raft.id server-2 --raft.bind 127.0.0.1:8528 -p 8527 -r /tmp/skv2

.PHONY: clean
clean:
	-rm out/skvcli
	-rm out/skv
	go clean
