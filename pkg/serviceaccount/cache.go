package serviceaccount

import "sync"

// Credentials holds a service account username and password for gateway
// communication.
type Credentials struct {
	Username string
	Password string
}

// Cache is a thread-safe in-memory store mapping tenant IDs to their
// rotated service account credentials.
type Cache struct {
	mu    sync.RWMutex
	store map[string]*Credentials
}

// NewCache returns an initialised, empty Cache.
func NewCache() *Cache {
	return &Cache{
		store: make(map[string]*Credentials),
	}
}

// Get retrieves the credentials for tenantID. The second return value is false
// when no entry exists for that tenant.
func (c *Cache) Get(tenantID string) (*Credentials, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	creds, ok := c.store[tenantID]
	return creds, ok
}

// Set stores credentials for tenantID, replacing any previously cached entry.
func (c *Cache) Set(tenantID string, creds *Credentials) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.store[tenantID] = creds
}
