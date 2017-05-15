package robinhood

import (
	"bytes"
	"hash/adler32"
	"log"
)

const InitialSize = 256
const LoadFactorPercent = 90

type elem struct {
	key   []byte
	value []byte
	hash  uint32
}

type HashTable struct {
	buffer          []elem
	resizeThreshold int
	numElements     int
	mask            uint32
}

func NewHashTable() *HashTable {
	return &HashTable{
		buffer:          make([]elem, InitialSize, InitialSize),
		resizeThreshold: (InitialSize * LoadFactorPercent) / 100,
		numElements:     0,
		mask:            InitialSize - 1,
	}
}

func (h *HashTable) hashKey(key []byte) uint32 {
	hash := adler32.New()
	hash.Write(key)
	sum := hash.Sum32()
	sum &= 0x7fffffff
	if sum == 0 {
		sum |= 1
	}
	return sum
}

func (h *HashTable) grow() {
	log.Printf("Growing buffer")
	buf := make([]elem, 2*cap(h.buffer), 2*cap(h.buffer))
	copy(buf, h.buffer)
	h.buffer = buf
	h.resizeThreshold = (cap(h.buffer) * LoadFactorPercent) / 100
	h.mask = uint32(cap(h.buffer) - 1)
}

func (h *HashTable) isDeleted(hash uint32) bool {
	return (hash >> 31) != 0
}

func (h *HashTable) desiredPos(hash uint32) uint32 {
	pos := hash & h.mask
	log.Printf("Got position: %d, %d, %d", hash, h.mask, pos)
	return pos
}

func (h *HashTable) probeDistance(hash, slotIndex uint32) uint32 {
	return (slotIndex + uint32(cap(h.buffer)) - h.desiredPos(hash)) & h.mask
}

func (h *HashTable) elemHash(ix uint32) uint32 {
	log.Printf("Trying to get hash for pos: %d", ix)
	return h.buffer[ix].hash
}

func (h *HashTable) insertHelper(hash uint32, key, value []byte) {
	pos := h.desiredPos(hash)
	dist := uint32(0)
	e := elem{
		hash:  hash,
		key:   key,
		value: value,
	}
	for {
		if h.elemHash(pos) == 0 {
			h.buffer[pos] = elem{
				hash:  hash,
				key:   key,
				value: value,
			}
			return
		}
		existingElemProdDist := h.probeDistance(h.elemHash(pos), pos)
		if existingElemProdDist < dist {
			if h.isDeleted(h.elemHash(pos)) {
				h.buffer[pos] = e
				return
			}
			curr := h.buffer[pos]
			h.buffer[pos] = e
			e = curr
			dist = existingElemProdDist
		}
		pos = (pos + 1) & h.mask
		dist++
	}
}

func (h *HashTable) lookupIndex(key []byte) int {
	hash := h.hashKey(key)
	pos := h.desiredPos(hash)
	dist := uint32(0)
	for {
		if h.elemHash(pos) == 0 {
			return -1
		} else if dist > h.probeDistance(h.elemHash(pos), pos) {
			return -1
		} else if h.elemHash(pos) == hash && bytes.Equal(h.buffer[pos].key, key) {
			return int(pos)
		}
		pos = (pos + 1) & h.mask
		dist++
	}
}

func (h *HashTable) Insert(key, value []byte) {
	if h.numElements+1 >= h.resizeThreshold {
		h.grow()
	}
	h.insertHelper(h.hashKey(key), key, value)
	h.numElements++
}

func (h *HashTable) Find(key []byte) *[]byte {
	ix := h.lookupIndex(key)
	if ix != -1 {
		return &h.buffer[ix].value
	}
	return nil
}

func (h *HashTable) Erase(key []byte) bool {
	ix := h.lookupIndex(key)
	if ix == -1 {
		return false
	}
	h.buffer[ix].hash |= 0x80000000
	h.numElements--
	return true
}

func (h *HashTable) Size() int {
	return h.numElements
}

func (h *HashTable) AverageProbeCount() float32 {
	probeTotal := uint32(0)
	for i := 0; i < h.numElements; i++ {
		hash := h.elemHash(uint32(i))
		if hash != 0 && !h.isDeleted(hash) {
			probeTotal += h.probeDistance(hash, uint32(i))
		}
	}
	return float32(probeTotal / uint32(h.numElements))
}
