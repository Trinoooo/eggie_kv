package core

import (
	"github.com/Trinoooo/eggie_kv/storage/core/iface"
	"github.com/Trinoooo/eggie_kv/storage/core/ragdoll"
)

var BuilderMap = map[string]iface.Builder{
	"ragdoll": ragdoll.New,
}
