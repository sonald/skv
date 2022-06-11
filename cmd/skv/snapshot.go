package main

import (
	"log"

	"github.com/hashicorp/raft"
)

type SKVSnapshot struct {
	nd *KVNode
}

func (ss *SKVSnapshot) Persist(sink raft.SnapshotSink) error {
	log.Printf("Persist: sink(%s)\n", sink.ID())

	return ss.nd.db.MakeSnapshot(sink)
}

func (ss *SKVSnapshot) Release() {
}
