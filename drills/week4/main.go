package main

import (
	"errors"
	"fmt"
	"sync"
)

var (
	ErrInvalidAmount     = errors.New("amount must be positive")
	ErrAccountNotFound   = errors.New("account not found")
	ErrSameAccount       = errors.New("cannot transfer to the same account")
	ErrInsufficientFunds = errors.New("insufficient funds")
	errNotImplemented    = errors.New("not implemented")
)

// Account holds money in the smallest unit (paise), never as a float.
type Account struct {
	ID      int
	mu      sync.Mutex
	balance int64
}

// Bank owns a fixed set of accounts. The accounts map is filled once in
// NewBank and never written again, so reading it from many goroutines is safe
// without a lock. Each account's balance is protected by its own mutex.
type Bank struct {
	accounts map[int]*Account
}

// NewBank creates a bank with the given opening balances.
func NewBank(balances map[int]int64) (*Bank, error) {
	accounts := make(map[int]*Account, len(balances))
	for id, bal := range balances {
		if bal < 0 {
			return nil, fmt.Errorf("account %d: negative opening balance %d", id, bal)
		}
		accounts[id] = &Account{ID: id, balance: bal}
	}
	return &Bank{accounts: accounts}, nil
}

// Balance returns the current balance of one account.
func (b *Bank) Balance(id int) (int64, error) {
	// TODO: look up the account, read its balance safely
	return 0, errNotImplemented
}

// ---------------------------------------------------------------------------
// Stage 1: no locking at all. Write it, then watch -race catch it.
// ---------------------------------------------------------------------------

// TransferUnsafe moves amount from one account to another with no locking.
// It is deliberately broken.
func (b *Bank) TransferUnsafe(from, to int, amount int64) error {
	// TODO: validate, check funds, move the money. No mutexes.
	return errNotImplemented
}

// ---------------------------------------------------------------------------
// Stage 2: lock both accounts, "from" first. Write it, then watch it deadlock.
// ---------------------------------------------------------------------------

// TransferNaiveLock locks the "from" account, then the "to" account, then
// moves the money. It is deliberately broken.
func (b *Bank) TransferNaiveLock(from, to int, amount int64) error {
	// TODO: validate, lock from, lock to, check funds, move the money
	return errNotImplemented
}

// ---------------------------------------------------------------------------
// Stage 3: the real thing.
// ---------------------------------------------------------------------------

// Transfer moves amount from one account to another atomically. It is safe
// for any number of concurrent callers and never deadlocks.
func (b *Bank) Transfer(from, to int, amount int64) error {
	// TODO: validate
	// TODO: lock both accounts in a way that can never deadlock
	// TODO: check funds, move the money
	return errNotImplemented
}

func main() {
	bank, err := NewBank(map[int]int64{1: 1000, 2: 1000})
	if err != nil {
		fmt.Println("bank:", err)
		return
	}

	var wg sync.WaitGroup
	for range 1000 {
		wg.Add(2)
		go func() { defer wg.Done(); bank.Transfer(1, 2, 1) }()
		go func() { defer wg.Done(); bank.Transfer(2, 1, 1) }()
	}
	wg.Wait()

	a, _ := bank.Balance(1)
	c, _ := bank.Balance(2)
	fmt.Printf("account 1: %d, account 2: %d, total: %d (want 2000)\n", a, c, a+c)
}
