package robinhood

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHashTableInsert(t *testing.T) {
	h := NewHashTable()
	h.Insert([]byte("test"), []byte("test"))
	h.Insert([]byte("test"), []byte("test"))
	assert.True(t, bytes.Equal(*h.Find([]byte("test")), []byte("test")))
	assert.Equal(t, 2, h.Size(), "Check number of items")
}

func TestHashTableCollision(t *testing.T) {
	h := NewHashTable()
	h.Insert([]byte("costarring"), []byte("costarring"))
	h.Insert([]byte("liquid"), []byte("liquid"))
	assert.True(t, bytes.Equal(*h.Find([]byte("costarring")), []byte("costarring")))
	h.Erase([]byte("costarring"))
	h.Insert([]byte("costarring"), []byte("costarring"))
}

func TestHashTableErase(t *testing.T) {
	h := NewHashTable()
	h.Insert([]byte("test"), []byte("test"))
	assert.Equal(t, 1, h.Size(), "Check number of items")
	assert.True(t, h.Erase([]byte("test")))
	assert.Equal(t, 0, h.Size(), "Check number of items")
	assert.Nil(t, h.Find([]byte("test")), "Check nil response")
}

func TestHashTableGrow(t *testing.T) {
	h := NewHashTable()
	for i := 1; i <= 256; i++ {
		h.Insert([]byte(fmt.Sprintf("test%d", i)), []byte("test"))
		assert.Equal(t, i, h.Size(), "Check number of items")
	}
}
