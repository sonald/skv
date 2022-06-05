package main

import (
	"log"

	"github.com/hashicorp/raft"
)

type SKVSnapshot struct {
}

func (ss *SKVSnapshot) Persist(sink raft.SnapshotSink) error {
	log.Printf("Persist: sink(%s)\n", sink.ID())

	return sink.Close()
}

func (ss *SKVSnapshot) Release() {
}
