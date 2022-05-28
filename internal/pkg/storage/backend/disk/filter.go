package disk

import "github.com/sonald/skv/internal/pkg/storage"

type Filter interface {
	Set(key *storage.InternalKey)
	Check(key *storage.InternalKey) bool
}
