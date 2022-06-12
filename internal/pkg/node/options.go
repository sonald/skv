package node

type NodeOption func(kv *KVNode)

func WithDebug(b bool) NodeOption {
	return func(kv *KVNode) {
		kv.debug = b
	}
}

func WithRpcAddress(addr string) NodeOption {
	return func(kv *KVNode) {
		kv.rpcAddress = addr
	}
}

func WithStorageRoot(path string) NodeOption {
	return func(kv *KVNode) {
		kv.storageRoot = path
	}
}

func WithBootstrap(b bool) NodeOption {
	return func(kv *KVNode) {
		kv.bootstrap = b
	}
}

func WithBootstrapAddress(addr string) NodeOption {
	return func(kv *KVNode) {
		kv.bootstrapAddress = addr
	}
}

func WithBind(bind string) NodeOption {
	return func(kv *KVNode) {
		kv.bind = bind
	}
}

func WithID(id string) NodeOption {
	return func(kv *KVNode) {
		kv.serverId = id
	}
}
