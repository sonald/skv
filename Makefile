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

.PHONY: test_docker
test_docker: skv skvcli
	docker run --name node1 sonald/skv
	docker run --name node2 sonald/skv /skv -c /node.yaml --raft.bootstrapAddress 172.17.0.2:9527 --raft.id server-2

.PHONY: compose
compose:
	docker compose -f scripts/docker-compose.yml down
	docker compose -f scripts/docker-compose.yml up

.PHONY: build_img	
build_img: scripts/Dockerfile 
	docker build -t sonald/skv -f scripts/Dockerfile .

.PHONY: clean
clean:
	-rm out/skvcli
	-rm out/skv
	go clean
