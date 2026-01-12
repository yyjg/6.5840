package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	key      string
	clientID string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l, clientID: kvtest.RandValue(8)}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			ver = 0
		}
		if val == "" {
			putErr := lk.ck.Put(lk.key, lk.clientID, ver)
			switch putErr {
			case rpc.ErrMaybe:
				if val, _, err := lk.ck.Get(lk.key); err == rpc.OK && lk.clientID == val {
					return
				}
			case rpc.OK:
				return
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, ver, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			return
		}
		if val == lk.clientID {
			putErr := lk.ck.Put(lk.key, "", ver)
			if putErr == rpc.OK {
				return
			} else if putErr == rpc.ErrMaybe {
				continue
			}
		} else {
			break
		}
	}
}
