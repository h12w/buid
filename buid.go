/*
Package buid provides Bipartite Unique Identifier (BUID)

A BUID is a 128-bit unique ID composed of two 64-bit parts: shard and key.

It is not only a unique ID, but also contains the sharding information, so that
the messages with the same BUID could be stored together within the same DB shard.

Also, when a message is stored in a shard, the shard part of the BUID can be
trimmed off to save the space, and only the key part needs to be stored as the
primary key.

Bigendian is chosen to make each part byte-wise lexicographic sortable.

BUID = shard key .

shard:

    0             1               2               3
    7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |           shard-index         |            reserved           |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |                  hours (from bespoke epoch)                   |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

key:

    0             1               2               3
    7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0 7 6 5 4 3 2 1 0
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
   |  minutes  |  seconds  |             nanoseconds
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
        nanoseconds    |  counter  |            process            |
   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+

Definitions:

   | item        | type   | description                                                        |
   |-------------|--------|--------------------------------------------------------------------|
   | shard-index | uint16 | the index of the shard for storing the data associated to the BUID |
   | hours       | uint32 | hours from bespoke epoch (490,293 years, should be enough :-)      |
   | minutes     | uint6  | 0-59 minutes within an hour                                        |
   | seconds     | uint6  | 0-59 seconds within a minute                                       |
   | nanoseconds | uint30 | 0-999999999 nanoseconds within a second                            |
   | counter     | uint6  | cyclic counter for within each nanosecond                          |
   | process     | uint16 | a unique process on a specific node                                |

*/
package buid

import (
	"sync"
	"time"
)

type (
	// ID is BUID
	ID [16]byte
	// Shard part of the BUID
	Shard [8]byte
	// Key part of the BUID
	Key [8]byte

	// Process represents a unique process on a specific node
	Process struct {
		id      uint16
		t       int64
		counter uint8
		mu      sync.Mutex
	}
)

const (
	secondInNano = 1000000000
	minuteInNano = 60 * secondInNano
	hourInNano   = 60 * minuteInNano
	maxCounter   = 0x3f
)

// Epoch is the bespoke epoch of BUID in Unix Epoch in nanoseconds
var Epoch = time.Date(2017, 10, 24, 0, 0, 0, 0, time.UTC).UnixNano()

// internalTime returns internal epoch time in nanoseconds
func internalTime(t time.Time) int64 {
	return t.UnixNano() - Epoch
}
func externalTime(t int64) time.Time {
	return time.Unix(0, Epoch+t).UTC()
}

// NewProcess returns a new Process object for id
func NewProcess(id uint16) *Process {
	// the internal time is added by a nanosecond to avoid
	// possible conflict caused by restarting within a nanosecond
	// (though not likely)
	return &Process{
		id: id,
		t:  internalTime(time.Now().Add(time.Nanosecond)),
	}
}

// NewID generates a new BUID from a shard index and a timestamp
func (p *Process) NewID(shard uint16, timestamp time.Time) ID {
	ts := internalTime(timestamp)

	// The implementation tries its best to avoid duplication:
	// 1. When p.t is in a fixed nanosecond, counter increases
	// 2. When p.t proceeds, counter resets
	// 3. When counter overflowed, wait until p.t can be updated to a later time
	// 4. Internal p.t never rewinds
	p.mu.Lock()
	for {
		if ts > p.t {
			p.t = ts
			p.counter = 0
		} else if p.counter > maxCounter {
			ts = internalTime(time.Now())
			continue
		}
		break
	}
	t := p.t
	counter := uint16(p.counter)
	p.counter++
	p.mu.Unlock()

	var (
		hour    = uint32(t / hourInNano)
		minute  = uint8((t % hourInNano) / minuteInNano)
		second  = uint8((t % minuteInNano) / secondInNano)
		nano    = uint32(t % secondInNano)
		process = p.id
	)

	return ID{
		// shard
		byte(shard >> 8), byte(shard),
		0, 0, // reserved
		byte(hour >> 24), byte(hour >> 16), byte(hour >> 8), byte(hour),

		// key
		((minute & 0x3f) << 2) | ((second & 0x30) >> 4),
		((second & 0x0f) << 4) | byte(nano>>26),
		byte(nano >> 18), byte(nano >> 10),
		byte(nano >> 2), byte(nano<<6) | byte(counter),
		byte(process >> 8), byte(process),
	}
}

// Time returns the embedded timestamp
func (id ID) Time() time.Time {
	var (
		hour = (uint32(id[4]) << 24) |
			(uint32(id[5]) << 16) |
			(uint32(id[6]) << 8) |
			uint32(id[7])
		minute = (id[8] & 0xfc) >> 2
		second = ((id[8] & 0x03) << 4) | (id[9] >> 4)
		nano   = (uint32(id[9]&0x0f) << 26) |
			(uint32(id[10]) << 18) |
			(uint32(id[11]) << 10) |
			(uint32(id[12]) << 2) |
			(uint32(id[13]) >> 6)
		t = int64(hour)*hourInNano +
			int64(minute)*minuteInNano +
			int64(second)*secondInNano +
			int64(nano)
	)
	return externalTime(t)
}

// Shard returns the embedded shard index
func (id ID) Shard() uint16 {
	return (uint16(id[0]) << 8) | uint16(id[1])
}

// Process returns the embedded process ID
func (id ID) Process() uint16 {
	return (uint16(id[14]) << 8) | uint16(id[15])
}

// Counter returns the embedded counter part of the key
func (id ID) Counter() uint16 {
	return uint16(id[13] & 0x3f)
}

// Split splits BUID to Shard and Key
func (id ID) Split() (Shard, Key) {
	var shard Shard
	var key Key
	copy(shard[:], id[:8])
	copy(key[:], id[8:])
	return shard, key
}

func join(shard Shard, key Key) ID {
	var id ID
	copy(id[:8], shard[:])
	copy(id[8:], key[:])
	return id
}

// Index returns the embedded shard index
func (s Shard) Index() uint16 {
	return join(s, Key{}).Shard()
}

// Time returns the embedded hours in time.Time
func (s Shard) Time() time.Time {
	return join(s, Key{}).Time()
}

// Time returns the embedded time in time.Duration
func (k Key) Time() time.Duration {
	t := join(Shard{}, k).Time()
	return t.Sub(t.Truncate(time.Hour))
}

// Process returns the embedded process ID
func (k Key) Process() uint16 {
	return join(Shard{}, k).Process()
}

// Counter returns the embedded counter part of the key
func (k Key) Counter() uint16 {
	return join(Shard{}, k).Counter()
}
