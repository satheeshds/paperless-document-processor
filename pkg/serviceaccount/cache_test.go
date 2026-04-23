package serviceaccount_test

import (
	"sync"
	"testing"

	"paperless-document-processor/pkg/serviceaccount"
)

func TestCache_SetAndGet(t *testing.T) {
	c := serviceaccount.NewCache()

	creds := &serviceaccount.Credentials{Username: "svc-user", Password: "svc-pass"}
	c.Set("tenant-1", creds)

	got, ok := c.Get("tenant-1")
	if !ok {
		t.Fatal("expected entry to exist after Set")
	}
	if got.Username != "svc-user" {
		t.Errorf("expected username svc-user, got %s", got.Username)
	}
	if got.Password != "svc-pass" {
		t.Errorf("expected password svc-pass, got %s", got.Password)
	}
}

func TestCache_GetMissing(t *testing.T) {
	c := serviceaccount.NewCache()
	_, ok := c.Get("nonexistent-tenant")
	if ok {
		t.Fatal("expected miss for unknown tenant, got hit")
	}
}

func TestCache_SetOverwrites(t *testing.T) {
	c := serviceaccount.NewCache()
	c.Set("tenant-2", &serviceaccount.Credentials{Username: "old", Password: "old-pass"})
	c.Set("tenant-2", &serviceaccount.Credentials{Username: "new", Password: "new-pass"})

	got, ok := c.Get("tenant-2")
	if !ok {
		t.Fatal("expected entry after overwrite")
	}
	if got.Username != "new" {
		t.Errorf("expected overwritten username new, got %s", got.Username)
	}
}

func TestCache_ConcurrentAccess(t *testing.T) {
	c := serviceaccount.NewCache()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		tenantID := "tenant-concurrent"
		go func() {
			defer wg.Done()
			c.Set(tenantID, &serviceaccount.Credentials{Username: "u", Password: "p"})
		}()
		go func() {
			defer wg.Done()
			c.Get(tenantID)
		}()
	}
	wg.Wait()
}
