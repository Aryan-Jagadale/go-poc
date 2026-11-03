package main

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var errNotImplemented = errors.New("not implemented")

// Clock returns the current time. Production passes time.Now; tests pass a
// fake clock they can move forward by hand.
type Clock func() time.Time

// ---------------------------------------------------------------------------
// Part A: token bucket
// ---------------------------------------------------------------------------

// TokenBucket allows up to burst requests at once, refilling at rate tokens
// per second. It is safe for concurrent use.
type TokenBucket struct {
	// TODO: fields
	rate       float64
	burst      int
	clock      Clock
	tokens     float64
	lastRefill time.Time
	mu         sync.Mutex
}

// NewTokenBucket returns a bucket that starts full (burst tokens).
func NewTokenBucket(rate float64, burst int, now Clock) (*TokenBucket, error) {
	// TODO: validate input
	if rate <= 0 {
		return nil, errors.New("rate must be positive")
	}
	if burst <= 0 {
		return nil, errors.New("burst must be positive")
	}
	if now == nil {
		return nil, errors.New("clock cannot be nil")
	}
	// TODO: build the bucket

	bucket := &TokenBucket{
		rate:       rate,
		burst:      burst,
		clock:      now,
		tokens:     float64(burst),
		lastRefill: now(),
	}

	return bucket, nil
}

// Allow reports whether one request may proceed right now, consuming a token
// if so.
func (b *TokenBucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Refill tokens based on time elapsed since the last refill, capped at burst
	now := b.clock()
	elapsed := now.Sub(b.lastRefill)
	if elapsed < 0 {
		elapsed = 0
	}
	tokensToAdd := elapsed.Seconds() * b.rate
	b.tokens += tokensToAdd
	if b.tokens > float64(b.burst) {
		b.tokens = float64(b.burst)
	}
	b.lastRefill = now

	// Take a token if one is available
	if b.tokens >= 1 {
		b.tokens -= 1
		return true
	}
	return false
}

// ---------------------------------------------------------------------------
// Part B: per-user limiter
// ---------------------------------------------------------------------------

// PerUserLimiter gives every user their own TokenBucket, and forgets users
// who have been idle for longer than idleTTL. It is safe for concurrent use.
type userEntry struct {
	bucket   *TokenBucket
	lastSeen time.Time
}

type PerUserLimiter struct {
	// TODO: fields
	mu    sync.Mutex
	users map[string]*userEntry

	rate  float64
	burst int

	idleTTL time.Duration
	clock   Clock

	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// NewPerUserLimiter starts a background cleanup that runs every cleanupEvery
// and removes users idle for longer than idleTTL. Call Close to stop it.
func NewPerUserLimiter(rate float64, burst int, idleTTL, cleanupEvery time.Duration, now Clock) (*PerUserLimiter, error) {
	// TODO: validate input
	if rate <= 0 {
		return nil, errors.New("rate must be positive")
	}
	if burst <= 0 {
		return nil, errors.New("burst must be positive")
	}
	if idleTTL <= 0 {
		return nil, errors.New("idleTTL must be positive")
	}
	if cleanupEvery <= 0 {
		return nil, errors.New("cleanupEvery must be positive")
	}
	if now == nil {
		return nil, errors.New("clock cannot be nil")
	}

	limiter := &PerUserLimiter{
		users:   make(map[string]*userEntry),
		rate:    rate,
		burst:   burst,
		idleTTL: idleTTL,
		clock:   now,
		stop:    make(chan struct{}),
		done:    make(chan struct{}),
	}

	go func() {
		defer close(limiter.done)
		ticker := time.NewTicker(cleanupEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				limiter.mu.Lock()
				now := limiter.clock()
				for userID, entry := range limiter.users {
					if now.Sub(entry.lastSeen) > limiter.idleTTL {
						delete(limiter.users, userID)
					}
				}
				limiter.mu.Unlock()
			case <-limiter.stop:
				return
			}
		}
	}()

	return limiter, nil

}

// Allow reports whether userID may make one request right now.
func (l *PerUserLimiter) Allow(userID string) bool {
	// TODO: find or create this user's bucket, then ask it
	l.mu.Lock()

	now := l.clock()
	entry, exists := l.users[userID]

	if !exists {
		bucket, err := NewTokenBucket(l.rate, l.burst, l.clock)
		if err != nil {
			l.mu.Unlock()

			return false // should not happen
		}
		entry = &userEntry{
			bucket:   bucket,
			lastSeen: now,
		}
		l.users[userID] = entry
	} else {
		entry.lastSeen = now
	}
	l.mu.Unlock()

	return entry.bucket.Allow()
}

// Len returns how many users are currently tracked.
func (l *PerUserLimiter) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()

	lenU := len(l.users)

	return lenU
}

// Close stops the cleanup goroutine and waits for it to exit. Calling Close
// more than once is safe.
func (l *PerUserLimiter) Close() {

	l.once.Do(func() {
		close(l.stop)
		<-l.done
	})
}

func main() {
	b, err := NewTokenBucket(10, 10, time.Now)
	if err != nil {
		fmt.Println("bucket:", err)
		return
	}

	allowed, denied := 0, 0
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if b.Allow() {
			allowed++
		} else {
			denied++
		}
		time.Sleep(time.Millisecond)
	}
	// Expect about 10 (initial burst) + 10/sec * 2s = ~30 allowed.
	fmt.Printf("allowed %d, denied %d in 2s\n", allowed, denied)
}
