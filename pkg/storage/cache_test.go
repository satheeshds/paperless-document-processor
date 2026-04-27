package storage

import (
	"sync"
	"testing"
	"time"
)

// newTestCache creates a cache with the given TTL for testing.
func newTestCache(ttl time.Duration) *saCache {
	return &saCache{
		entries: make(map[string]cachedServiceAccount),
		ttl:     ttl,
	}
}

func TestSACache_MissOnEmpty(t *testing.T) {
	c := newTestCache(5 * time.Minute)
	_, ok := c.get("tenant-1")
	if ok {
		t.Fatal("expected cache miss on empty cache")
	}
}

func TestSACache_HitAfterSet(t *testing.T) {
	c := newTestCache(5 * time.Minute)
	sa := &serviceAccount{Username: "svc_user", Password: "svc_pass"}
	c.set("tenant-1", sa)

	got, ok := c.get("tenant-1")
	if !ok {
		t.Fatal("expected cache hit after set")
	}
	if got.Username != sa.Username || got.Password != sa.Password {
		t.Errorf("cached credentials mismatch: got %+v, want %+v", got, sa)
	}
}

func TestSACache_MissAfterExpiry(t *testing.T) {
	c := newTestCache(10 * time.Millisecond)
	sa := &serviceAccount{Username: "svc_user", Password: "svc_pass"}
	c.set("tenant-1", sa)

	time.Sleep(20 * time.Millisecond)

	_, ok := c.get("tenant-1")
	if ok {
		t.Fatal("expected cache miss after TTL expiry")
	}
}

func TestSACache_Invalidate(t *testing.T) {
	c := newTestCache(5 * time.Minute)
	sa := &serviceAccount{Username: "svc_user", Password: "svc_pass"}
	c.set("tenant-1", sa)

	c.invalidate("tenant-1")

	_, ok := c.get("tenant-1")
	if ok {
		t.Fatal("expected cache miss after invalidation")
	}
}

func TestSACache_IsolateTenants(t *testing.T) {
	c := newTestCache(5 * time.Minute)
	sa1 := &serviceAccount{Username: "user1", Password: "pass1"}
	sa2 := &serviceAccount{Username: "user2", Password: "pass2"}
	c.set("tenant-1", sa1)
	c.set("tenant-2", sa2)

	got1, ok1 := c.get("tenant-1")
	got2, ok2 := c.get("tenant-2")
	if !ok1 || !ok2 {
		t.Fatal("expected both tenants to be cached")
	}
	if got1.Username != "user1" || got2.Username != "user2" {
		t.Errorf("tenant credentials crossed: tenant-1=%+v, tenant-2=%+v", got1, got2)
	}
}

func TestSACache_ConcurrentAccess(t *testing.T) {
	c := newTestCache(5 * time.Minute)
	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(n int) {
			defer wg.Done()
			tenantID := "tenant-concurrent"
			sa := &serviceAccount{Username: "user", Password: "pass"}
			c.set(tenantID, sa)
			_, _ = c.get(tenantID)
			c.invalidate(tenantID)
		}(i)
	}
	wg.Wait()
	// No data race — test passes if the race detector doesn't trigger.
}

func TestSACache_SetOverwritesExistingEntry(t *testing.T) {
	c := newTestCache(5 * time.Minute)
	c.set("tenant-1", &serviceAccount{Username: "old_user", Password: "old_pass"})
	c.set("tenant-1", &serviceAccount{Username: "new_user", Password: "new_pass"})

	got, ok := c.get("tenant-1")
	if !ok {
		t.Fatal("expected cache hit after overwrite")
	}
	if got.Username != "new_user" {
		t.Errorf("expected overwritten credentials, got %+v", got)
	}
}
