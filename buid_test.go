package buid

import (
	"fmt"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestTime(t *testing.T) {
	process := NewProcess(1)
	ts := time.Now().UTC().Add(time.Nanosecond)
	id := process.NewID(2, ts)
	if extractedTs := id.Time(); !extractedTs.Equal(ts) {
		t.Fatalf("expect %v got %v", ts, extractedTs)
	}
}

func TestCounterReset(t *testing.T) {
	process := NewProcess(1)
	ts := time.Now().UTC()
	var id ID
	for i := 0; i < 5; i++ {
		id = process.NewID(2, ts)
	}
	_, key := id.Split()
	if key.Counter() == 0 {
		t.Fatal("expect counter > 0")
	}

	ts = ts.Add(-time.Millisecond)
	id = process.NewID(2, ts)
	_, key = id.Split()
	if key.Counter() == 0 {
		t.Fatal("expect counter still > 0, not reset")
	}

	ts = ts.Add(time.Millisecond)
	id = process.NewID(2, ts)
	_, key = id.Split()
	if key.Counter() == 0 {
		t.Fatal("expect counter still > 0, not reset")
	}

	ts = ts.Add(time.Millisecond)
	id = process.NewID(2, ts)
	_, key = id.Split()
	if key.Counter() > 0 {
		t.Fatal("expect counter is reset")
	}
}

func TestCounterOverflow(t *testing.T) {
	var id ID
	process := NewProcess(1)
	ts := externalTime(process.t)

	for i := 0; i <= maxCounter; i++ {
		id = process.NewID(2, ts)
		_, key := id.Split()
		if int(key.Counter()) != i {
			t.Fatalf("expect counter %d got %d", i, key.Counter())
		}
		if !ts.Equal(id.Time()) {
			t.Fatalf("expect time %v got %v", ts, id.Time())
		}
	}

	// get the first ID based on the overflowed counter
	id = process.NewID(2, ts)
	_, key := id.Split()
	if key.Counter() != 0 {
		t.Fatalf("expect 0 got %d", key.Counter())
	}

	expectedTs := externalTime(process.t)
	if !expectedTs.After(ts) {
		t.Fatal("expect the ts proceed")
	}
	if extractedTs := id.Time(); !extractedTs.Equal(expectedTs) {
		t.Fatalf("expect %v got %v", ts, extractedTs)
	}
}

func BenchmarkNewID(b *testing.B) {
	process := NewProcess(1)
	t := time.Now()

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		id := process.NewID(2, t)
		_ = id
		t = t.Add(time.Nanosecond)
	}
}

func TestShardIndex(t *testing.T) {
	process := NewProcess(1)
	id := process.NewID(42, time.Now())
	shard, _ := id.Split()
	if shard.Index() != 42 {
		t.Fatalf("expect 42 got %d", shard.Index())
	}
}

func TestShardTime(t *testing.T) {
	ts := time.Now().UTC()
	hour := ts.Truncate(time.Hour)
	process := NewProcess(1)
	id := process.NewID(42, ts)
	shard, _ := id.Split()
	if !shard.Time().Equal(hour) {
		t.Fatalf("expect %v got %v", hour, shard.Time())
	}
}

func TestKeyTime(t *testing.T) {
	process := NewProcess(1)
	ts := externalTime(process.t)
	id := process.NewID(42, ts)
	_, key := id.Split()
	expected := ts.Sub(ts.Truncate(time.Hour))
	if key.Time() != expected {
		t.Fatalf("expect %v got %v", expected, key.Time())
	}
}

func TestKeyProcess(t *testing.T) {
	ts := time.Now().UTC()
	process := NewProcess(12)
	id := process.NewID(42, ts)
	_, key := id.Split()
	if key.Process() != 12 {
		t.Fatalf("expect 12 got %v", key.Process())
	}
}

func TestKeyCounterInc(t *testing.T) {
	ts := time.Now().UTC()
	process := NewProcess(12)
	var id ID
	for i := 0; i < 23; i++ {
		id = process.NewID(42, ts)
	}
	_, key := id.Split()
	if key.Counter() != 22 {
		t.Fatalf("expect 22 got %v", key.Counter())
	}
}

func TestUniqueness(t *testing.T) {
	process := NewProcess(12)
	var wg sync.WaitGroup
	n := runtime.NumCPU()
	idss := make([][]ID, n)
	for i := 0; i < n; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids := make([]ID, 100000)
			for j := range ids {
				ids[j] = process.NewID(1, time.Now())
			}
			idss[i] = ids
		}()
	}
	wg.Wait()
	m := make(map[ID]bool)
	for _, ids := range idss {
		for _, id := range ids {
			if m[id] {
				t.Fatal("duplication detected")
			}
			m[id] = true
		}
	}
}

func BenchmarkMaxCounter(b *testing.B) {
	process := NewProcess(12)
	var a []int
	var mu sync.Mutex
	b.RunParallel(func(pb *testing.PB) {
		c := int(0)
		for pb.Next() {
			id := process.NewID(1, time.Now())
			_, key := id.Split()
			if int(key.Counter()) > c {
				c = int(key.Counter())
			}
		}
		mu.Lock()
		a = append(a, c)
		mu.Unlock()
	})
	sort.Sort(sort.Reverse(sort.IntSlice(a)))
	b.Logf("max counter is %d", a[0])
}

func TestMarshalText(t *testing.T) {
	id1 := NewProcess(2).NewID(1, time.Now().UTC())
	buf, err := id1.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	var id2 ID
	if err := id2.UnmarshalText(buf); err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		for i := range id1 {
			if id1[i] != id2[i] {
				fmt.Printf("%d,%x,%x\n", i, id1[i], id2[i])
			}
		}
		t.Fatalf("expect\n%x\ngot\n%x", id1[:], id2[:])
	}
}

func TestMarshalTextForKey(t *testing.T) {
	id1 := NewProcess(2).NewID(1, time.Now().UTC())
	_, key1 := id1.Split()
	buf, err := key1.MarshalText()
	if err != nil {
		t.Fatal(err)
	}
	var key2 Key
	if err := key2.UnmarshalText(buf); err != nil {
		t.Fatal(err)
	}
	if key1 != key2 {
		for i := range key1 {
			if key1[i] != key2[i] {
				fmt.Printf("%d,%x,%x\n", i, key1[i], key2[i])
			}
		}
		t.Fatalf("expect\n%x\ngot\n%x", key1[:], key2[:])
	}
}

func TestUnmarshalTextError(t *testing.T) {
	{
		buf := make([]byte, 31)
		var id ID
		if err := id.UnmarshalText(buf); err == nil {
			t.Fatal("expect error")
		}
	}
	{
		buf := []byte("ff")
		var id ID
		if err := id.UnmarshalText(buf); err == nil {
			t.Fatal("expect error")
		}
	}
}

func TestString(t *testing.T) {
	id := NewProcess(2).NewID(1, externalTime(Epoch))
	expected := "0skIcr10rnBGT3wdrHO2"
	if id.String() != expected {
		t.Fatalf("expect %v got %v", expected, id.String())
	}
}

func TestIsZero(t *testing.T) {
	var id ID
	if !id.IsZero() {
		t.Fatal("expect zero is true")
	}
	id[1] = 1
	if id.IsZero() {
		t.Fatal("expect zero is not true")
	}
}
