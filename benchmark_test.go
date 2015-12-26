package hbase

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	. "github.com/pingcap/check"
)

type BenchTestSuit struct {
	cli       HBaseClient
	tableName string
}

var _ = Suite(&BenchTestSuit{})

func (s *BenchTestSuit) SetUpTest(c *C) {
	var err error
	s.cli, err = NewClient(getTestZkHosts(), "/hbase")
	c.Assert(err, IsNil)

	s.tableName = "test_bench"
	tblDesc := NewTableDesciptor(s.tableName)
	cf := newColumnFamilyDescriptor("cf", 1)
	tblDesc.AddColumnDesc(cf)
	s.cli.CreateTable(tblDesc, nil)
}

func (s *BenchTestSuit) TearDownTest(c *C) {
	c.Assert(s.cli, NotNil)

	err := s.cli.DisableTable(s.tableName)
	c.Assert(err, IsNil)

	err = s.cli.DropTable(s.tableName)
	c.Assert(err, IsNil)
}

func (s *BenchTestSuit) put(c *C, idx int64, prefix string) {
	key := fmt.Sprintf("%s-%d", prefix, idx)
	p := NewPut([]byte(key))
	p.AddValue([]byte("cf"), []byte("q"), []byte(key))
	ok, err := s.cli.Put(s.tableName, p)
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)
}

func (s *BenchTestSuit) delete(c *C, idx int64, prefix string) {
	key := fmt.Sprintf("%s-%d", prefix, idx)
	d := NewDelete([]byte(key))
	d.AddColumn([]byte("cf"), []byte("q"))
	ok, err := s.cli.Delete(s.tableName, d)
	c.Assert(err, IsNil)
	c.Assert(ok, IsTrue)
}

func (s *BenchTestSuit) get(c *C, idx int64, prefix string) {
	key := fmt.Sprintf("%s-%d", prefix, idx)
	g := NewGet([]byte(key))
	g.AddColumn([]byte("cf"), []byte("q"))
	rs, err := s.cli.Get(s.tableName, g)
	c.Assert(err, IsNil)
	c.Assert(string(rs.SortedColumns[0].Value), Equals, key)
}

func (s *BenchTestSuit) TestPutBenchmark(c *C) {
	countSlice := []int{10000, 100000, 100000, 100000, 100000, 100000}
	workerCountSlice := []int{1, 10, 20, 50, 100, 200}

	idx := int64(0)
	for k, workerCount := range workerCountSlice {
		now := time.Now()
		count := countSlice[k]
		jobCount := count / workerCount
		var wg sync.WaitGroup
		wg.Add(workerCount)
		for i := 0; i < workerCount; i++ {
			go func(i int) {
				defer wg.Done()
				for j := 0; j < jobCount; j++ {
					id := atomic.AddInt64(&idx, 1)
					s.put(c, id, "put")
				}
			}(i)
		}
		wg.Wait()

		cost := time.Since(now).Nanoseconds() / 1e6
		tps := int64(count) * 1e3 / cost
		fmt.Printf("TestPutBenchmark --> %d threads %d puts, cost %d ms, tps %d\n", workerCount, count, cost, tps)
	}
}

func (s *BenchTestSuit) TestGetBenchmark(c *C) {
	idx := int64(0)
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 10000; j++ {
				id := atomic.AddInt64(&idx, 1)
				s.put(c, id, "get")
			}
		}(i)
	}
	wg.Wait()

	countSlice := []int{10000, 100000, 100000, 100000, 100000, 100000}
	workerCountSlice := []int{1, 10, 20, 50, 100, 200}

	for k, workerCount := range workerCountSlice {
		now := time.Now()
		count := countSlice[k]
		jobCount := count / workerCount
		var wg sync.WaitGroup
		wg.Add(workerCount)
		for i := 0; i < workerCount; i++ {
			go func(i int) {
				defer wg.Done()
				for j := 0; j < jobCount; j++ {
					id := 1 + rand.Int63n(idx)
					s.get(c, id, "get")
				}
			}(i)
		}
		wg.Wait()

		cost := time.Since(now).Nanoseconds() / 1e6
		tps := int64(count) * 1e3 / cost
		fmt.Printf("TestGetBenchmark --> %d threads %d gets, cost %d ms, tps %d\n", workerCount, count, cost, tps)
	}
}

func (s *BenchTestSuit) TestDeleteBenchmark(c *C) {
	countSlice := []int{10000, 100000, 100000, 100000, 100000, 100000}
	workerCountSlice := []int{1, 10, 20, 50, 100, 200}

	idx := int64(0)
	for k, workerCount := range workerCountSlice {
		count := countSlice[k]
		jobCount := count / workerCount
		var wg sync.WaitGroup
		wg.Add(workerCount)
		for i := 0; i < workerCount; i++ {
			go func(i int) {
				defer wg.Done()
				for j := 0; j < jobCount; j++ {
					id := atomic.AddInt64(&idx, 1)
					s.put(c, id, "delete")
				}
			}(i)
		}
		wg.Wait()

		now := time.Now()
		idx = int64(0)
		wg.Add(workerCount)
		for i := 0; i < workerCount; i++ {
			go func(i int) {
				defer wg.Done()
				for j := 0; j < jobCount; j++ {
					id := atomic.AddInt64(&idx, 1)
					s.delete(c, id, "delete")
				}
			}(i)
		}
		wg.Wait()

		cost := time.Since(now).Nanoseconds() / 1e6
		tps := int64(count) * 1e3 / cost
		fmt.Printf("TestDeleteBenchmark --> %d threads %d deletes, cost %d ms, tps %d\n", workerCount, count, cost, tps)
	}
}

func (s *BenchTestSuit) TestScanBenchmark(c *C) {
	idx := int64(0)
	workerCount := 10
	count := 100000
	jobCount := count / workerCount

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < jobCount; j++ {
				id := atomic.AddInt64(&idx, 1)
				s.put(c, id, "scan")
			}
		}(i)
	}
	wg.Wait()

	cacheSlice := []int{1, 10, 20, 50, 100, 200, 500, 1000}
	for _, cache := range cacheSlice {
		now := time.Now()
		idx = int64(0)
		scan := NewScan([]byte(s.tableName), cache, s.cli)
		defer scan.Close()

		for {
			r := scan.Next()
			if r == nil || scan.Closed() {
				break
			}

			c.Assert(r.SortedColumns[0].Value, BytesEquals, r.Row)
			idx++
		}
		c.Assert(idx, Equals, int64(count))

		cost := time.Since(now).Nanoseconds() / 1e6
		tps := int64(count) * 1e3 / cost
		fmt.Printf("TestScanBenchmark --> %d kvs %d cache cost %d ms, tps %d\n", count, cache, cost, tps)
	}
}

func (s *BenchTestSuit) TestFilterScanBenchmark(c *C) {
	idx := int64(0)
	workerCount := 10
	count := 100000
	jobCount := count / workerCount
	defaultPrefix := "default-scan"
	testPrefix := "prefix-scan"
	prefixSlice := []string{defaultPrefix, testPrefix}
	totalCount := len(prefixSlice) * count

	var wg sync.WaitGroup
	wg.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func(i int) {
			defer wg.Done()
			for j := 0; j < jobCount; j++ {
				id := atomic.AddInt64(&idx, 1)
				for k := range prefixSlice {
					s.put(c, id, prefixSlice[k])
				}
			}
		}(i)
	}
	wg.Wait()

	cacheSlice := []int{1, 10, 20, 50, 100, 200, 500, 1000}
	for _, cache := range cacheSlice {
		{
			now := time.Now()
			idx = int64(0)
			scan := NewScan([]byte(s.tableName), cache, s.cli)
			defer scan.Close()

			for {
				r := scan.Next()
				if r == nil || scan.Closed() {
					break
				}

				c.Assert(r.SortedColumns[0].Value, BytesEquals, r.Row)
				idx++
			}
			c.Assert(idx, Equals, int64(totalCount))

			cost := time.Since(now).Nanoseconds() / 1e6
			tps := int64(totalCount) * 1e3 / cost
			fmt.Printf("TestFilterScanBenchmark --> %d kvs %d cache no filter cost %d ms, tps %d\n", totalCount, cache, cost, tps)
		}
		{
			now := time.Now()
			idx = int64(0)
			scan := NewScan([]byte(s.tableName), cache, s.cli)
			filter := NewPrefixFilter([]byte(testPrefix))
			scan.AddFilter(filter)
			defer scan.Close()

			for {
				r := scan.Next()
				if r == nil || scan.Closed() {
					break
				}

				c.Assert(r.SortedColumns[0].Value, BytesEquals, r.Row)
				idx++
			}
			c.Assert(idx, Equals, int64(count))

			cost := time.Since(now).Nanoseconds() / 1e6
			tps := int64(totalCount) * 1e3 / cost
			fmt.Printf("TestFilterScanBenchmark --> %d kvs %d cache prefix filter cost %d ms, tps %d\n", totalCount, cache, cost, tps)
		}
	}
}
