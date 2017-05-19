package robinhood

import (
	"bytes"
	"hash/fnv"
)

const InitialSize = 256
const LoadFactorPercent = 98

type elem struct {
	key   []byte
	value interface{}
	hash  uint32
}

type HashTable struct {
	buffer          []elem
	resizeThreshold uint32
	numElements     uint32
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

func (h *HashTable) capacity() uint32 {
	return uint32(cap(h.buffer))
}

func (h *HashTable) hashKey(key []byte) uint32 {
	hash := fnv.New32a()
	hash.Write(key)
	sum := hash.Sum32()
	sum &= uint32(0x7fffffff)
	if sum == 0 {
		sum |= 1
	}
	return sum
}

func (h *HashTable) grow() {
	newCap := 2 * h.capacity()
	buf := make([]elem, newCap, newCap)
	copy(buf, h.buffer)
	h.buffer = buf
	h.resizeThreshold = (newCap * LoadFactorPercent) / 100
	h.mask = h.capacity() - 1
}

func (h *HashTable) isDeleted(hash uint32) bool {
	return hash>>31 != 0
}

func (h *HashTable) desiredPos(hash uint32) uint32 {
	return hash & h.mask
}

func (h *HashTable) probeDistance(hash, slotIndex uint32) uint32 {
	return (slotIndex + h.capacity() - h.desiredPos(hash)) & h.mask
}

func (h *HashTable) elemHash(ix uint32) uint32 {
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
		q := h.elemHash(pos)
		if q == 0 {
			h.buffer[pos] = e
			return
		}
		existingElemProdDist := h.probeDistance(q, pos)
		if existingElemProdDist < dist {
			if h.isDeleted(q) {
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
		q := h.elemHash(pos)
		if q == 0 {
			return -1
		} else if dist > h.probeDistance(q, pos) {
			return -1
		} else if q == hash && bytes.Equal(h.buffer[pos].key, key) {
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

func (h *HashTable) Find(key []byte) interface{} {
	ix := h.lookupIndex(key)
	if ix != -1 {
		return h.buffer[ix].value
	}
	return nil
}

func (h *HashTable) Erase(key []byte) bool {
	ix := h.lookupIndex(key)
	if ix == -1 {
		return false
	}
	h.buffer[ix].hash = uint32(0x80000000)
	h.numElements--
	return true
}

func (h *HashTable) Size() int {
	return int(h.numElements)
}

func (h *HashTable) AverageProbeCount() float32 {
	probeTotal := uint32(0)
	for i := 0; i < int(h.capacity()); i++ {
		hash := h.elemHash(uint32(i))
		if hash != 0 && !h.isDeleted(hash) {
			probeTotal += h.probeDistance(hash, uint32(i))
		}
	}
	return float32(probeTotal / h.numElements)
}
