package disk

type Filter interface {
	Set(key string)
	Check(key string) bool
}
