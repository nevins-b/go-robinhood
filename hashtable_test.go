package robinhood

import (
	"bufio"
	"bytes"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHashTableInsert(t *testing.T) {
	h := NewHashTable()
	h.Insert([]byte("test"), []byte("test"))
	h.Insert([]byte("test"), []byte("test"))
	assert.True(t, bytes.Equal(h.Find([]byte("test")).([]byte), []byte("test")))
	assert.Equal(t, 2, h.Size(), "Check number of items")
}

func TestHashTableCollision(t *testing.T) {
	h := NewHashTable()
	h.Insert([]byte("costarring"), []byte("costarring"))
	h.Insert([]byte("liquid"), []byte("liquid"))
	assert.True(t, bytes.Equal(h.Find([]byte("costarring")).([]byte), []byte("costarring")))
	h.Erase([]byte("costarring"))
	assert.True(t, bytes.Equal(h.Find([]byte("liquid")).([]byte), []byte("liquid")))
	h.Insert([]byte("costarring"), []byte("costarring"))
	h.Insert([]byte("costarring"), []byte("costarring1"))
	h.Erase([]byte("costarring"))
	h.Insert([]byte("costarring"), []byte("costarring2"))
	h.Insert([]byte("costarring"), []byte("costarring3"))
}

func TestHashTableErase(t *testing.T) {
	h := NewHashTable()
	h.Insert([]byte("test"), []byte("test"))
	assert.Equal(t, 1, h.Size(), "Check number of items")
	assert.True(t, h.Erase([]byte("test")))
	assert.Equal(t, 0, h.Size(), "Check number of items")
	assert.Nil(t, h.Find([]byte("test")), "Check nil response")
	h.Insert([]byte("test"), []byte("test"))
}

func TestHashTableGrow(t *testing.T) {
	file, err := os.Open("100k.txt")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	h := NewHashTable()
	scanner := bufio.NewScanner(file)
	start := time.Now()
	for scanner.Scan() {
		h.Insert(scanner.Bytes(), scanner.Bytes())
	}
	log.Printf("Insert took: %s", time.Since(start))
	file.Seek(0, 0)
	scanner = bufio.NewScanner(file)
	start = time.Now()
	i := 0
	max := 10000
	for scanner.Scan() {
		h.Erase(scanner.Bytes())
		i++
		if i >= max {
			break
		}
	}
	log.Printf("Erase took: %s", time.Since(start))
	file.Seek(0, 0)
	scanner = bufio.NewScanner(file)
	start = time.Now()
	for scanner.Scan() {
		h.Find(scanner.Bytes())
	}
	log.Printf("Lookup took: %s", time.Since(start))
	log.Printf("Average Probe Count: %f", h.AverageProbeCount())
}
