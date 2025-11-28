from minikv import MiniKV, MiniKVConfig, WriteMode

cfg = MiniKVConfig(write_mode=WriteMode.SYNC)
kv = MiniKV(cfg)
