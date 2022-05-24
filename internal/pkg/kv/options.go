package kv

type KVOption func(kv *KVImpl) KVOption

func WithRoot(path string) KVOption {
	return func(kv *KVImpl) KVOption {
		prev := kv.root
		kv.root = path
		return WithRoot(prev)
	}
}

func WithDumpPolicy(p int) KVOption {
	return func(kv *KVImpl) KVOption {
		prev := kv.dumpPolicy
		kv.dumpPolicy = p
		return WithDumpPolicy(prev)
	}
}

func WithDumpSizeThreshold(i int) KVOption {
	return func(kv *KVImpl) KVOption {
		prev := kv.dumpSizeThreshold
		kv.dumpSizeThreshold = i
		return WithDumpSizeThreshold(prev)
	}
}

func WithDumpCountThreshold(i int) KVOption {
	return func(kv *KVImpl) KVOption {
		prev := kv.dumpCountThreshold
		kv.dumpCountThreshold = i
		return WithDumpCountThreshold(prev)
	}
}

func WithDebug(v bool) KVOption {
	return func(kv *KVImpl) KVOption {
		prev := kv.debug
		kv.debug = v
		return WithDebug(prev)
	}
}
