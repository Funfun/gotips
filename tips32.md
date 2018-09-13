## #31 - Do not need any web framework 
> 2016-14-03 by [@beyondns](https://github.com/beyondns)  

You don't need any web framework(s) at all, golang net/http and other standard lib modules are enough to build almost any net/web app with support of udp/tcp/http1/2/websocket!

## #30 - Heart beat and lead election with etcd 
> 2016-13-03 by [@beyondns](https://github.com/beyondns)  

Lead election is very simple with etcd just use prevExist and ttl params for concurent http api calls from different nodes.

```go
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"
	"math/rand"
)

var (
	etcdKeys = "http://127.0.0.1:2379/v2/keys/"
)

func httpRequest(meth, u, val string, timeLimit time.Duration) (int, []byte, error) {

	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	c := make(chan error, 1)

	var respStatus int
	var respBody []byte

	req, err := http.NewRequest(meth, u, strings.NewReader(val))
	if err != nil {
		return 0, nil, err
	}

	if val != "" {
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	}

	go func() {
		resp, err := client.Do(req)

		if err != nil {
			goto E
		}

		respStatus = resp.StatusCode

		respBody, err = ioutil.ReadAll(resp.Body)
	E:
		c <- err
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	select {
	case <-time.After(timeLimit):
		tr.CancelRequest(req)
		log.Printf("Request timeout")
		<-c // Wait for goroutine to return.
		return 0, nil, errors.New("request time out")
	case err := <-c:
		if err != nil {
			log.Printf("Error in request goroutine %v", err)
			return 0, nil, err
		}
		return respStatus, respBody, nil
	}

}

//curl -L -X PUT http://127.0.0.1:2379/v2/keys/message -d value="Hello"
//{"action":"set","node":{"key":"/message","value":"Hello","modifiedIndex":4,"createdIndex":4}}

func etcdSet(k, v string) (int, []byte, error) {
	return httpRequest("PUT", etcdKeys+k, "value="+v, time.Second)
}

func etcdSetTTL(k, v string, ttl int) (int, []byte, error) {
	return httpRequest("PUT", etcdKeys+k, fmt.Sprintf("value=%s&ttl=%d", v, ttl), time.Second)
}

func etcdSetIfNotExistsTTL(k, v string, ttl int) (int, []byte, error) {
	return httpRequest("PUT", etcdKeys+k, fmt.Sprintf("value=%s&ttl=%d&prevExist=false", v, ttl), time.Second)
}

//curl -L http://127.0.0.1:2379/v2/keys/message
//{"action":"get","node":{"key":"/message","value":"Hello","modifiedIndex":4,"createdIndex":4}}

func etcdGet(k string) (int, []byte, error) {
	return httpRequest("GET", etcdKeys+k, "", time.Second)
}

// curl -L http://127.0.0.1:2379/v2/keys/foo-service?wait=true\&recursive=true
func etcdWait(k string) (int, []byte, error) {
	return httpRequest("GET", etcdKeys+k+"?wait=true", "", time.Second*3600)
}

const (
	HeartBeatInterval = time.Second * 5
	LeadBeatInterval  = time.Second * 5
	NodeTTL           = 10
)

type Node struct {
	Name            string
	URL             string
	HeartBeatCancel chan struct{}
	LeadBeatCancel  chan struct{}
	ElectionCancel  chan struct{}
}

func NewNode(name, url string) *Node {
	return &Node{
		Name:            name,
		URL:             url,
		HeartBeatCancel: make(chan struct{},1),
		LeadBeatCancel:  make(chan struct{},1),
		ElectionCancel:  make(chan struct{},1),
	}
}

func (n *Node) HeartBeat() {
	for {
		select {
		case <-time.After(HeartBeatInterval):
			log.Printf("node %s heart beat", n.Name)
			etcdSetTTL("Nodes/"+n.Name, n.URL, NodeTTL)
		case <-n.HeartBeatCancel:
			log.Printf("node %s heart beat canceled", n.Name)
			return
		}
	}
}

func (n *Node) LeadBeat() {
	for {
		select {
		case <-time.After(LeadBeatInterval):
			log.Printf("node %s lead beat", n.Name)
			etcdSetTTL("Lead", n.URL, NodeTTL)
		case <-n.LeadBeatCancel:
			log.Printf("node %s lead beat canceled", n.Name)
			return

		}
	}
}

func (n *Node) Election() {
	for {
		select {
		case <-time.After(HeartBeatInterval):
			s, _, err := etcdSetIfNotExistsTTL("Lead", n.URL, NodeTTL)
			if err != nil {
				log.Printf("node %s Election error %v", n.Name, err)
				continue
			}
			if s == 201 {
				log.Printf("node %s is new Lead", n.Name)
				n.HeartBeatCancel <- struct{}{}
				go n.LeadBeat()
				return
			}
		case <-n.ElectionCancel:
			log.Printf("node %s election canceled", n.Name)
			return
		}
	}
}

func (n *Node) Kill() {
	n.HeartBeatCancel <- struct{}{}
	n.LeadBeatCancel <- struct{}{}
	n.ElectionCancel <- struct{}{}
}

func (n *Node) Run() {
	go n.HeartBeat()
	go n.Election()
}

func main() {
	h := flag.String("host", "127.0.0.1", "host")
	p := flag.Int("port", 8000, "port")
	flag.Parse()

	var nodes []*Node

	for i := 0; i < 3; i++ {
		n := NewNode(fmt.Sprintf("node%d", i), fmt.Sprintf("%s:%d", *h, (*p)+i))
		go n.Run()
		nodes = append(nodes, n)
	}
	for {
		select {
		case <-time.After(time.Second * 15):
			i := rand.Intn(len(nodes))
			n := nodes[i]
			name := n.Name
			url := n.URL
			log.Printf("Kill random node %s", name)
			go n.Kill()

			time.Sleep(time.Second)

			n = NewNode(name, url)
			go n.Run()
			nodes[i] = n
		}
	}
}
```
```bash

```


## #29 - Partial json read 
> 2016-13-03 by [@beyondns](https://github.com/beyondns)  

Use only fields you need in json struct for faster parsing. Use json:"-" to exclude field.

```go
package main

import (
	"math/rand"
	"encoding/json"
	"testing"
	"fmt"
)

var (

jsonData  = []string{`
{
	"id":123123123,
	"name":"hero",
	"bigdata":{
		"a" : "wejjhkajshdfkjsahgfhasdkjhfaskd",
		"b" : "ajndajsdbkjsadhfgajsdfhasdghags",
		"c" : "asdnmkcakdlaksdkaskdlakdkakdass" 
	}
}
`,
`
{
	"id":0695096,
	"name":"warrior",
	"bigdata":{
		"a" : "sjdnkjsbfvkjdsbfvkjdfb",
		"b" : "snvdsfnvdfnv,dnfvdnfnvdfnv,dnfv,d",
		"c" : "dcsfdcbmndfbvcndbfvdbfmvb" 
	}
}
`,
`
{
	"id":3823233,
	"name":"superman",
	"bigdata":{
		"a" : "qnqw,neqw,enq,wenq,wen",
		"b" : "snvdsfnvdfnv,dnfvdnfnvdfnv,dnfv,d",
		"c" : "dcvnfmvnd,vgn,n" 
	}
}
`,
}
)

type BigData struct{
	A string `json:"a"`
	B string `json:"b"`
	C string `json:"c"`

}

type DataFull struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	BD BigData  `json:"bigdata"`
}

func BenchmarkFullJson(b *testing.B) {
	rand.Seed(376234242)
    for i := 0; i < b.N; i++ {
        var d DataFull
        j := rand.Intn(len(jsonData))
		json.Unmarshal([]byte(jsonData[j]), &d)
    }    
}

type DataShort struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
	//BD BigData  `json:"-"`
}

func TestData(t *testing.T) {
    var ds DataShort
	json.Unmarshal([]byte(jsonData[0]), &ds)
	//ds.BD.A="a"
	fmt.Printf("%v\n",ds)
    var df DataFull
	json.Unmarshal([]byte(jsonData[0]), &df)
	fmt.Printf("%v\n",df)
}


func BenchmarkShortJson(b *testing.B) {
	rand.Seed(376234242)
    for i := 0; i < b.N; i++ {
        var d DataShort
        j := rand.Intn(len(jsonData))
		json.Unmarshal([]byte(jsonData[j]), &d)
    }    
}
```
```bash
go test -bench=.
testing: warning: no tests to run
PASS
BenchmarkFullJson-2 	   50000	     34626 ns/op
BenchmarkShortJson-2	   50000	     24630 ns/op
```

* [using-golang-and-json-for-kafka-consumption-with-high-throughput](https://medium.com/the-hoard/using-golang-and-json-for-kafka-consumption-with-high-throughput-4cae28e08f90)

## #28 - Interact with etcd with http.Request 
> 2016-10-03 by [@beyondns](https://github.com/beyondns)  

Don't use [client](https://github.com/coreos/etcd/tree/master/client) but use http.Request to get more control.  

Place WAL to ramdisk to save HDD from very frequet writes (development mode only) 
```bash
sudo mkdir /mnt/ramdisk/
sudo mount -t tmpfs -o size=32M tmpfs /mnt/ramdisk/
etcd --wal-dir /mnt/ramdisk
```

```go
package main

import (
	"errors"
	"log"
	"time"
	"io/ioutil"
	"strings"
	"net/http"
)

const (
	ResponseWaitTimeLimit = time.Second*10
)

var (
	etcdKeys = "http://127.0.0.1:2379/v2/keys/"
)

func httpRequest(meth, u, val string, timeLimit time.Duration) (int,[]byte, error) {

	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	c := make(chan error, 1)

	var respStatus int
	var respBody []byte

	req, err := http.NewRequest(meth, u, strings.NewReader(val))
	if err != nil {
		return 0,nil, err
	}

	if val!=""{
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	}

	go func() {
		resp, err := client.Do(req)

		if err != nil {
			goto E
		}

		respStatus = resp.StatusCode

		respBody, err = ioutil.ReadAll(resp.Body)
	E:
		c <- err
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()

	select {
	case <-time.After(timeLimit):
		tr.CancelRequest(req)
		log.Printf("Request timeout")
		<-c // Wait for goroutine to return.
		return 0,nil, errors.New("request time out")
	case err := <-c:
		if err != nil {
			log.Printf("Error in request goroutine %v", err)
			return 0,nil, err
		}
		return respStatus,respBody, nil
	}

}


//curl -L -X PUT http://127.0.0.1:2379/v2/keys/message -d value="Hello"
//{"action":"set","node":{"key":"/message","value":"Hello","modifiedIndex":4,"createdIndex":4}}

func etcdSet(k, v string) (int,[]byte, error) {
	return httpRequest("PUT", etcdKeys+k, "value="+v, time.Second)
}


//curl -L http://127.0.0.1:2379/v2/keys/message
//{"action":"get","node":{"key":"/message","value":"Hello","modifiedIndex":4,"createdIndex":4}}

func etcdGet(k string) (int,[]byte, error) {
	return httpRequest("GET", etcdKeys+k, "", time.Second)
}

// curl -L http://127.0.0.1:2379/v2/keys/foo-service?wait=true\&recursive=true
func etcdWait(k string) (int,[]byte, error) {
	return httpRequest("GET", etcdKeys+k+"?wait=true", "", time.Second*3600)
}

func main() {

	c := make(chan bool)

	go func() {

		s, d, err := etcdWait("foo")
		if err != nil {
			log.Fatalf("etcdWait error %v", err)
		}
		log.Printf("wait %d %s", s, string(d))

		c <- true
	}()

	time.Sleep(time.Second) // wait etcdWait 

	s, d, err := etcdSet("foo", "bar")
	if err != nil {
		log.Fatalf("etcdSet error %v", err)
	}
	log.Printf("set %d %s", s, string(d))

	s, d, err = etcdGet("foo")
	if err != nil {
		log.Fatalf("etcdGet error %v", err)
	}
	log.Printf("get %d %s", s, string(d))

	<-c
}

```
* [getting-started-with-etcd](https://coreos.com/etcd/docs/latest/getting-started-with-etcd.html)
* [api](https://github.com/coreos/etcd/blob/master/Documentation/api.md)

## #27 - Go-style concurrency in C
> 2016-04-03 by [@beyondns](https://github.com/beyondns)

* [libmill](https://github.com/sustrik/libmill)
* [libconcurrent](https://github.com/sharow/libconcurrent)

## #26 - Go channels are slow
> 2016-03-03 by [@beyondns](https://github.com/beyondns)

* [go-channels-are-bad-and-you-should-feel-bad](http://www.jtolds.com/writing/2016/03/go-channels-are-bad-and-you-should-feel-bad/)
* [proposal: improve channels for M:N producer/consumer scenarios](https://github.com/golang/go/issues/14601)
* [github.com/eapache/channels](https://github.com/eapache/channels)
* [interesting-ways-of-using-go-channels](http://nomad.so/2016/01/interesting-ways-of-using-go-channels/)

## #25 - Avoid conversions with hidden alloc copy
> 2016-03-03 by [@beyondns](https://github.com/beyondns)  

Avoid []byte - string conversion, for example use io.WriteString(s) instead of Write([]byte(string))


## #24 - Slice shuffle
> 2016-26-02 by [@beyondns](https://github.com/beyondns)

```go
package main

import (
	"fmt"
	"math/rand"
	"time"
)

func shuffle(s []string) {
	t0 := int64(time.Now().Nanosecond())
	rand.Seed(t0)
	fmt.Printf("%v", t0)

	for i := len(s) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		s[i], s[j] = s[j], s[i]
	}
}

func main() {

	s := []string{"a", "b", "c"}
	for i := 0; i < 3; i++ {
		shuffle(s)
		fmt.Println(s)
	}
}

```
```bash
go run shuffle.go 
322916783[c a b]
323220313[b a c]
323351126[a c b]

```

## #23 - Defer is slow
> 2016-25-02 by [@beyondns](https://github.com/beyondns)

```go
import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

func doF(v *uint32, l *sync.RWMutex) {
	(*l).Lock()
	(*v)++
	(*l).Unlock()
}

func doFD(v *uint32, l *sync.RWMutex) {
	(*l).Lock()
	defer (*l).Unlock()
	(*v)++
}

func BenchmarkLockDefer(b *testing.B) {
	var l sync.RWMutex
	var counter uint32 = 0
	runtime.GOMAXPROCS(runtime.NumCPU())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			doFD(&counter, &l)
		}
	})
	fmt.Printf("counter = %d\n", counter)
}

func BenchmarkLock(b *testing.B) {
	var l sync.RWMutex
	var counter uint32 = 0
	runtime.GOMAXPROCS(runtime.NumCPU())
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			doF(&counter, &l)
		}
	})
	fmt.Printf("counter = %d\n", counter)
}

```

```bash
go test -bench=.

BenchmarkLockDefer-2	counter = 1
counter = 100
counter = 10000
counter = 1000000
 1000000	      1073 ns/op
BenchmarkLock-2     	counter = 1
counter = 100
counter = 10000
counter = 1000000
counter = 5000000
 5000000	       249 ns/op
```

* [so-you-wanna-go-fast](http://bravenewgeek.com/so-you-wanna-go-fast/)

## #22 - Any number of args
> 2016-23-02 by [@beyondns](https://github.com/beyondns)

```go
func f(args ...string) []string {
         var r []string
         for _, s := range args {
                 r = append(r, s)
         }
         return r
}

func main(){
	fmt.Prinln(f("alpha","beta","gama")) // ["alpha","beta","gama"]
}
```
* [golang-accept-any-number-of-function-arguments-with-three-dots](https://socketloop.com/tutorials/golang-accept-any-number-of-function-arguments-with-three-dots)

## #21 - Test bench http request handler
> 2016-21-02 by [@beyondns](https://github.com/beyondns)


server.go
```go
package main

import (
	"log"
	"net/http"
)

func handleHttpRq(w http.ResponseWriter, r *http.Request) {
	//	log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte("Hello world"))
}

func main() {
	http.HandleFunc("/", handleHttpRq)
	log.Fatal(http.ListenAndServe("127.0.0.1:8000", nil))
}

```

server_test.go
```go
package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

const host = "http://127.0.0.1:8000/"

func TestRequest(t *testing.T) {

	req, err := http.NewRequest("GET", host, nil)
	if err != nil {
		t.Fatal(err)
	}

	rw := httptest.NewRecorder()

	handleHttpRq(rw, req)

	if rw.Code == 500 {
		t.Fatal("Internal server Error: " + rw.Body.String())
	}
	if rw.Body.String() != "Hello world" {
		t.Fatal("Expected " + rw.Body.String())
	}

}

func BenchmarkRequest(b *testing.B) {
	req, err := http.NewRequest("GET", host, nil)
	if err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i++ {
		rw := httptest.NewRecorder()
		handleHttpRq(rw, req)
	}

}
```
* [Daily code optimization](https://medium.com/@hackintoshrao/daily-code-optimization-using-benchmarks-and-profiling-in-golang-gophercon-india-2016-talk-874c8b4dc3c5#.bh2vh8tjf)

## #20 - Use Atomics or GOMAXPROCS=1
> 2016-18-02 by [@beyondns](https://github.com/beyondns)


```go
import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

func BenchmarkLock(b *testing.B) {
	var l sync.RWMutex
	var counter uint32 = 0
	runtime.GOMAXPROCS(runtime.NumCPU())
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			l.Lock()
			counter++
			l.Unlock()
		}
	})
	fmt.Printf("counter = %d\n", counter)
}

func BenchmarkAtomic(b *testing.B) {
	var counter uint32 = 0
	runtime.GOMAXPROCS(runtime.NumCPU())
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			atomic.AddUint32(&counter, 1)
		}
	})
	fmt.Printf("counter = %d\n", counter)
}

func BenchmarkX(b *testing.B) {
	var counter uint32 = 0
	runtime.GOMAXPROCS(1)
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			counter++
		}
	})
	fmt.Printf("counter = %d\n", counter)
}

```

```bash
BenchmarkLock-2  	counter = 1
counter = 100
counter = 10000
counter = 1000000
counter = 5000000
 5000000	       261 ns/op	       0 B/op	       0 allocs/op
BenchmarkAtomic-2	counter = 1
counter = 100
counter = 10000
counter = 1000000
counter = 50000000
50000000	        35.4 ns/op	       0 B/op	       0 allocs/op
BenchmarkX-2     	counter = 1
counter = 100
counter = 10000
counter = 1000000
counter = 50000000
50000000	        35.5 ns/op	       0 B/op	       0 allocs/op
```

## #19 - Chunked HTTP response with flusher 
> 2016-18-02 by [@beyondns](https://github.com/beyondns)

```go
type Forever struct{}

func (s *Forever) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL)

	closeNotify := w.(http.CloseNotifier).CloseNotify()
	flusher := w.(http.Flusher)

	w.WriteHeader(http.StatusOK)
	flusher.Flush()
	for {
		select {
		case <-closeNotify:
			return
		case <-time.After(3 * time.Second):
			fmt.Fprintf(w, "%v\n", time.Now())
			flusher.Flush()
		}
	}

}

func main() {
	p := flag.String("port", "8000", "port")
	flag.Parse()
	log.Fatal(http.ListenAndServe(":"+(*p), &Forever{}))
}

```

```bash
curl -v http://localhost:8000/
*   Trying 127.0.0.1...
* Connected to localhost (127.0.0.1) port 8000 (#0)
> GET / HTTP/1.1
> Host: localhost:8000
> User-Agent: curl/7.46.0
> Accept: */*
> 
< HTTP/1.1 200 OK
< Date: Thu, 18 Feb 2016 13:08:29 GMT
< Content-Type: text/plain; charset=utf-8
< Transfer-Encoding: chunked
< 
2016-02-18 16:08:32.482807768 +0300 MSK
2016-02-18 16:08:35.483426765 +0300 MSK
2016-02-18 16:08:38.484006772 +0300 MSK

```

## #18 - Use for range for channels 
> 2016-17-02 by [@beyondns](https://github.com/beyondns)


```go
const N = 3

func main() {
	ch := make(chan int, N)
	d := make(chan struct{})
	
	// use for range 
	go func() {
		for i := range ch {
			fmt.Printf("get %d\n", i)
		}
		fmt.Println("get done")
		close(d)
	}()

	// use for
	/*
	go func() {
		c := 0
		for c < N {
			fmt.Printf("get %d\n", <-ch)
			c++
		}
		fmt.Println("get done")
		close(d)
	}()
	*/

	// use for select
	/*
	go func() {
		c := 0
		for c < N {
			select {
			case <-time.After(time.Second):
				panic("time out")
			case i := <-ch:
				c++
				fmt.Printf("get %d\n", i)
			}
		}
		fmt.Println("get done")
		close(d)
	}()
	*/

	go func() {
		for i := 0; i < N; i++ {
			ch <- i
		}
		close(ch)
		fmt.Println("put done")
	}()
	time.Sleep(time.Microsecond)
	<-d
	fmt.Println("main done")
}

```
* [run](https://play.golang.org/p/t6t-ANNsJH)
* [github.com/valyala/fasthttp/blob/master/workerpool.go](https://github.com/valyala/fasthttp/blob/master/workerpool.go#L191)


## #17 - Use context API
> 2016-16-02 by [@beyondns](https://github.com/beyondns)

Some APIs are designed with context interface, google search is an example. Use context to send cancel signal.

```go
	done := make(chan error,1)
	query := "golang context"
	ctx, cancel := context.WithCancel(context.Background())
	// or use ctx, cancel = context.WithTimeout(context.Background(), queryTimeLimit)
	go func() {
		start := time.Now()
		_, err := google.Search(ctx, query)
		elapsed := time.Since(start)
		fmt.Printf("search time %v", elapsed)
		done <- err
	}()
	select {
	case <-time.After(queryTimeLimit):
		cancel()
		<-done // wait
		fmt.Printf("time out")
	case err :=<- done:
		if err != nil {
			panic(err)
		}
	}	
	fmt.Printf("Done")
```

* [blog.golang.org/context](https://blog.golang.org/context)
* [blog.golang.org/context/server/server.go](https://blog.golang.org/context/server/server.go)

## #16 - Go routines syncronization
> 2016-16-02 by [@beyondns](https://github.com/beyondns)

* Wait groups
```go
	const N = 3
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func(i int) {
			defer wg.Done()
			fmt.Println(i)
		}(i)
	}
	wg.Wait()
	fmt.Println("Done")
```
[run](https://play.golang.org/p/0LLtAQk6Lm)


* Channels
```go
	const N = 3
	done := make(chan struct{})
	for i := 0; i < N; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			fmt.Println(i)
		}(i)
	}
	var counter = 0
	for {
		select {
		case <-time.After(time.Second * 1):
			panic("time out")
		case <-done:
			counter++
			if counter == N {
				goto DONE
			}
		}
	}
DONE:
	fmt.Println("Done")
```
[run](https://play.golang.org/p/88J0jUm95v)

* [how-to-wait-for-all-goroutines-to-finish-executing-before-continuing](http://nathanleclaire.com/blog/2014/02/15/how-to-wait-for-all-goroutines-to-finish-executing-before-continuing/)


## #15 - Time interval measurement
> 2016-14-02 by [@beyondns](https://github.com/beyondns)

```go
  start := time.Now()
  DoHardWork() // or time.Sleep(1 * time.Second)
  finish := time.Since(start)
  fmt.Printf("Hard work finished in %d ms.\n", finish.Nanoseconds()/10e6)
``` 

## #14 - Benchmark switch vs else if
> 2016-14-02 by [@beyondns](https://github.com/beyondns)

Switch vs else if no big difference

```go
package boom

import (
	"fmt"
	"math/rand"
	"testing"
)

var words = []string{
	"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta",
	"iota", "kappa", "lambda", "mu", "nu", "xi", "omicron", "pi", "rho",
	"sigma", "tau", "upsilon", "phi", "chi", "psi", "omega",
}
var wlen = len(words)

func BenchmarkSwitch(b *testing.B) {
	m := make(map[string]int)
	rand.Seed(376234242)
	for i := 0; i < b.N; i++ {
		j := rand.Intn(wlen)
		s := words[j]
		switch s {
		case "alpha":
			m[s]++
		case "beta":
			m[s]++
		case "gamma":
			m[s]++
		case "delta":
			m[s]++
		case "epsilon":
			m[s]++
		case "zeta":
			m[s]++
		case "eta":
			m[s]++
		case "theta":
			m[s]++
		case "iota":
			m[s]++
		case "kappa":
			m[s]++
		case "lambda":
			m[s]++
		case "mu":
			m[s]++
		case "nu":
			m[s]++
		case "xi":
			m[s]++
		case "omicron":
			m[s]++
		case "pi":
			m[s]++
		case "rho":
			m[s]++
		case "sigma":
			m[s]++
		case "tau":
			m[s]++
		case "upsilon":
			m[s]++
		case "phi":
			m[s]++
		case "chi":
			m[s]++
		case "psi":
			m[s]++
		case "omega":
			m[s]++
		}
	}
	var c int
	for _,v:=range m{
		c=c+v
	}
	fmt.Printf("%d %d\n", len(m),c)
}

func BenchmarkIf(b *testing.B) {
	m := make(map[string]int)
	rand.Seed(376234242)
	for i := 0; i < b.N; i++ {
		j:= rand.Intn(wlen)
		w := words[j]
		if w == "alpha" {
			m[w]++
		} else if w == "beta" {
			m[w]++
		} else if w == "gamma" {
			m[w]++
		} else if w == "delta" {
			m[w]++
		} else if w == "epsilon" {
			m[w]++
		} else if w == "zeta" {
			m[w]++
		} else if w == "eta" {
			m[w]++
		} else if w == "theta" {
			m[w]++
		} else if w == "iota" {
			m[w]++
		} else if w == "kappa" {
			m[w]++
		} else if w == "lambda" {
			m[w]++
		} else if w == "mu" {
			m[w]++
		} else if w == "nu" {
			m[w]++
		} else if w == "xi" {
			m[w]++
		} else if w == "omicron" {
			m[w]++
		} else if w == "pi" {
			m[w]++
		} else if w == "rho" {
			m[w]++
		} else if w == "sigma" {
			m[w]++
		} else if w == "tau" {
			m[w]++
		} else if w == "upsilon" {
			m[w]++
		} else if w == "phi" {
			m[w]++
		} else if w == "chi" {
			m[w]++
		} else if w == "psi" {
			m[w]++
		} else if w == "omega" {
			m[w]++
		}
	}
	c:=0
	for _,v:=range m{
		c=c+v
	}
	fmt.Printf("%d %d\n", len(m),c)
}

```

```bash
go test -bench=.

BenchmarkSwitch-2	1 1
24 100
24 10000
24 1000000
 1000000	      1203 ns/op
BenchmarkIf-2    	1 1
24 100
24 10000
24 1000000
 1000000	      1214 ns/op

```
* [making-switch-cool-again](http://elliot.land/making-switch-cool-again)
* [golang.org/pkg/testing/](https://golang.org/pkg/testing/)
* [github.com/golang/go/issues/10000](https://github.com/golang/go/issues/10000)

## #13 - Use ASM in Go Code 
> 2016-10-02 by [@beyondns](https://github.com/beyondns)

Place add.go & add_asm.s in src/add sub folder, then build.

```assembly
// add_asm.s
TEXT add(SB),$0
        MOVL x+0(FP), BX
        MOVL y+4(FP), BP
        ADDL BP, BX
        MOVL BX, ret+8(FP)
        RET
```
```go
// main.go
func add(x, y int32) int32

func main() {
	r:=add(2,5)
	fmt.Println(r)
}
```

```bash
export GOPATH=$PWD 
go build add
```

To play with go/asm code compile
```bash
go tool compile -S code.go > code.s
```

* [golang.org/doc/asm](https://golang.org/doc/asm)
* [goroutines.com/asm](https://goroutines.com/asm)

## #12 - JSON with unknown structure
> 2016-08-02 by [@papercompute](https://github.com/papercompute)

Parse JSON with unknown structure, just pass map[string]interface{} to json.Unmarshal

```go
func ProcessJSON(s string) map[string]interface{} {
	var m map[string]interface{}

	if err := json.Unmarshal([]byte(s), &m); err != nil {
		panic(err)
	}

	if err := passM(m); err != nil {
		panic(err)
	}
	return m
}

func passKV(k string, v interface{}) error {
	var ok bool

	if _, ok = v.(map[string]interface{}); ok {
		err := passM(v.(map[string]interface{}))
		if err != nil {
			return err
		}
	}

	if _, ok = v.([]interface{}); ok {
		err := passA(v.([]interface{}))
		if err != nil {
			return err
		}
	}

	fmt.Printf("%s : %v\n", k, v)

	return nil
}

func passM(m map[string]interface{}) error {
	for k, v := range m {
		if err := passKV(k, v); err != nil {
			return err
		}
	}
	return nil
}

func passA(a []interface{}) error {
	for _, v := range a {
		switch v.(type) {
		case map[string]interface{}:
			if err := passM(v.(map[string]interface{})); err != nil {
				return err
			}
		case []interface{}:
			if err := passA(v.([]interface{})); err != nil {
				return err
			}
		default:
			return errors.New("wrong expression in array")
		}
	}
	return nil
}
```

## #11 - Websocket over HTTP2
> 2016-08-02 by [@beyondns](https://github.com/beyondns)

In go 1.6 HTTP2 is supported out of the box with http.ListenAndServeTLS and can be used with websockets. Just use wss connection. 

```go

func WSServer(ws *websocket.Conn) {
	var err error
	for {
		var reply string

		if err = websocket.Message.Receive(ws, &reply); err != nil {
			fmt.Println("Can't receive")
			break
		}

		fmt.Println("Received back from client: " + reply)

		msg := "Received:  " + reply
		fmt.Println("Sending to client: " + msg)

		if err = websocket.Message.Send(ws, msg); err != nil {
			fmt.Println("Can't send")
			break
		}
	}
}

func main() {
	http.Handle("/ws", websocket.Handler(WSServer))
	http.Handle("/", http.FileServer(http.Dir(".")))
	err := http.ListenAndServeTLS(":8000", "srv.cert", "srv.key", nil)
	if err != nil {
		log.Fatal(err)
	}
}
```

JS Client

```js
<script type="text/javascript">
    function WebSocketTest() {
        if ("WebSocket" in window) {
            console.log("WebSocket is supported by your Browser!");

            var ws = new WebSocket("wss://localhost:8000/ws");

            ws.onopen = function() {
                ws.send("Message from client");
                console.log("Message is sent...");
            };

            ws.onmessage = function(evt) {
                console.log("Message is received...", evt.data);
            };

            ws.onclose = function() {
                console.log("Connection is closed...");
            };
        } else {
            console.log("WebSocket NOT supported by your Browser!");
        }
    }
</script>
```

* [websocket](golang.org/x/net/websocket)


Brad Fitzpatrick ‏@bradfitz  Feb 19 Bengaluru, India
@activedaily the http2 working group forgot about websockets until the last second when it was too late. WS requires http1. Lame.

## #10 - HTTP2
> 2016-07-02 by [@beyondns](https://github.com/beyondns)

[Go 1.6's net/http package supports HTTP/2 for both the client and server out of the box.](https://github.com/golang/go/wiki/Go-1.6-release-party)  

In go 1.6+ HTTP2 is supported transparently, just use http.ListenAndServeTLS

Generate key & cert files with openssl
```bash
openssl req -x509 -newkey rsa:2048 -nodes -keyout srv.key -out srv.cert -days 365
```

HTTP2 server
```go
import (
	"fmt"
	"log"
	"flag"
	"net/http"	
	"golang.org/x/net/http2" // optional in go 1.6+
)

func Handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Hello http2")
}

func main() {
	var server http.Server

	host := flag.String("h", ":8000", "h host:port")
	flag.BoolVar(&http2.VerboseLogs, "verbose", false, "Verbose HTTP/2 debugging.")
	flag.Parse()

	server.Addr = *host
	http2.ConfigureServer(&server, nil)

	http.HandleFunc("/", Handler)

	log.Println("ListenAndServe on ", *host)

	log.Fatal(server.ListenAndServeTLS("srv.cert", "srv.key"))
}

```

CURL
```bash
curl --http2 --insecure https://localhost:8080
```

HTTP2 client
```go
	host := flag.String("h", "https://127.0.0.1:8000", "h host:port")

	flag.Parse()
	tr := &http.Transport{TLSClientConfig: tlsConfigInsecure}
	defer tr.CloseIdleConnections()

	req, err := http.NewRequest("GET", *host, nil)
	if err != nil {
		panic(err)
	}
	res, err := tr.RoundTrip(req)
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()

	log.Printf("Got res: %+v\n", res)
	if g, w := res.StatusCode, 200; g != w {
		panic(fmt.Sprintf("StatusCode = %v; want %v", g, w))
	}

	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		panic(fmt.Sprintf("Body read error: %v", err))
	}
	fmt.Printf("%s\n", string(data))

```
* [HTTP/2 and http2 in Go 1.6 video by Brad Fitzpatrick](https://www.youtube.com/watch?v=FARQMJndUn0)
* [h2demo](https://github.com/golang/net/blob/master/http2/h2demo)
* [tools-for-debugging-testing-and-using-http-2](https://blog.cloudflare.com/tools-for-debugging-testing-and-using-http-2/)
* [curl-with-http2-support](https://serversforhackers.com/video/curl-with-http2-support)

## #9 - Error handling
> 2016-06-02 by [@beyondns](https://github.com/beyondns)

Use last return variable in function as an error  
Use panic / defer / recover to handle more complex errors

```go

func MyFunc1(v interface{}) (interface{}, error) {
	var ok bool

	if !ok {
		return nil, errors.New("not ok error")
	}
	return v, nil
}

func MyFunc2() {
	defer func() {
		if err := recover(); err != nil {
			// recover from panic
			fmt.Println("recover: ", err)
		}
	}()
	v := struct{}{}
	if _, err := MyFunc1(v); err != nil {
		panic(err) // panic
	}

	fmt.Println("never happen")
}

func main() {
	MyFunc2()
	fmt.Println("main finish")
}

```

## #8 - Memory management with pools of objects
> 2016-04-02 by [@beyondns](https://github.com/beyondns)

Use thread safe pools to collect objects for reuse.

```go
var p sync.Pool
var o *T 
if v := p.Get(); v != nil {
	o = v.(*T)
} else {
	o = new(T)
}

// use o

p.Put(o) // return to reuse

```
* [pool](https://golang.org/src/sync/pool.go)
* [manual memory management](https://github.com/teh-cmc/mmm)

## #7 - Sort slice of time periods
> 2016-02-02 by [@beyondns](https://github.com/beyondns)

For custom data structures it's necessary to use custom compare function to sort elements in slice.

```go

type xTime struct {
	time.Time
}

func (t *xTime) UnmarshalJSON(buf []byte) error {
	tm, err := time.Parse("2006-01-02", strings.Trim(string(buf), `"`))
	if err != nil {
		return err
	}
	t.Time = tm
	return nil
}

type Period struct {
	Start xTime `json:"start"`
	End   xTime `json:"end"`
}

type Data struct {
	Ps []Period `json:"periods"`
}

type Periods []Period

func (a Periods) Len() int           { return len(a) }
func (a Periods) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a Periods) Less(i, j int) bool { return compDate(a[i].Start, a[j].Start) < 0 }

func compDate(a, b xTime) int {
	ay, am, ad := a.Time.Date()
	by, bm, bd := b.Time.Date()
	if ay > by {
		return 1
	}
	if ay < by {
		return -1
	}

	if am > bm {
		return 1
	}
	if am < bm {
		return -1
	}

	if ad > bd {
		return 1
	}
	if ad < bd {
		return -1
	}

	return 0
}

func main() {

	data := `
{"periods": [
    {"start": "2015-11-01", "end": "2015-11-01"},
    {"start": "2015-12-02", "end": "2015-12-03"},
    {"start": "2015-12-07", "end": "2015-12-13"},
    {"start": "2015-10-18", "end": "2016-10-30"}
  ]}
`
	var d Data
	if err := json.Unmarshal([]byte(data), &d); err != nil {
		panic(err)
	}
	sort.Sort(Periods(d.Ps))
	fmt.Printf("%v", d)

}

```

## #6 - Fast http server
> 2016-01-02 by [@beyondns](https://github.com/beyondns)

If you don't need net/http package functionality just use net and "tcp"

```go
package main

import (
	"flag"
	//"fmt"
	"io"
	"log"
	"net"
	"sync"
)

func main() {

	host := flag.String("h", "127.0.0.1:8000", "h host:port")

	flag.Parse()

	l, err := net.Listen("tcp", *host)
	if err != nil {
		panic(err)
	}

	log.Printf("Listening on %s", *host)

	for {
		conn, err := l.Accept()
		if err != nil {
			continue
		}

		go handleRequest(&conn)
	}

}

var rp sync.Pool

func handleRequest(c *net.Conn) {
	var rb *[]byte
	if v := rp.Get(); v != nil {
		rb = v.(*[]byte)
	} else {
		rb = new([]byte)
		*rb = make([]byte, 2048)
	}

	n, err := (*c).Read(*rb)
	if err != nil || n <= 0 {
		goto E
	}

	// parse http if necessary

	io.WriteString(*c, "HTTP/1.1 200 Ok\r\nConnection: close\r\nContent-Length: 5\r\n\r\nhello")
E:
	(*c).Close()

}


```
Just test it

```bash
./wrk -c 1024 -d 20s http://127.0.0.1:8000
```

* [fasthttp](https://github.com/valyala/fasthttp)
* [handling-1-million-requests-per-minute-with-golang](http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/)
* [from-tcp-to-tls-in-go](https://pascal.nextrem.ch/2015/12/17/from-tcp-to-tls-in-go/)

## #5 - Close channel to notify many
> 2016-28-01 by [@beyondns](https://github.com/beyondns)

```go
	c:=make(chan int)

	for i:=0;i<5;i++{
		go func(i int){ 
			_, ok :=<-c;
			fmt.Printf("closed %d, %t\n",i,ok) // random order
		}(i)
	}


	// c<-1
	close(c)
	time.Sleep(time.Second)	

```

## #4 - Is channel closed?
> 2016-28-01

```go
	c:=make(chan int)
	go func(){
		if _, ok :=<-c;ok{
			fmt.Println("not closed",<-c)
		} else {
			fmt.Println("closed",<-c)
		}
			
	}()
	// c<-1
	close(c)
	time.Sleep(time.Second)	

```

## #3 - Http request/response with close notify and timeout
> 2016-27-01 by [@beyondns](https://github.com/beyondns)
> Deprecated: the CloseNotifier interface predates Go's context package. New code should use Request.Context instead.

```go
const RequestMaxWaitTimeInterval = time.Second * 15

func Handler(w http.ResponseWriter, r *http.Request) {
	u:=r.URL.Query().Get("url")
	if u==""{
		http.Error(w, "url is not specified", http.StatusBadRequest)
		return
	}
	var err error
	u,err=url.QueryUnescape(u)
	if err!=nil{
		http.Error(w, fmt.Sprintf("url unescape error %v",err), http.StatusBadRequest)
		return
	}
	closeNotify := w.(http.CloseNotifier).CloseNotify()
	//flusher := w.(http.Flusher)

	tr := &http.Transport{}
	client := &http.Client{Transport: tr}
	c := make(chan error, 1)

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("error %v", err), http.StatusBadRequest)
		return
	}

	go func() {
		resp, err := client.Do(req)

		defer func() { c <- err }()
		defer func() {
			if resp != nil && resp.Body != nil {
				resp.Body.Close()
			}
		}()

		if err != nil {
			return
		}

		if resp.StatusCode != 200 {
			http.Error(w, fmt.Sprintf("resp.StatusCode %s", resp.Status), resp.StatusCode)
			return
		}

		data, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			http.Error(w, fmt.Sprintf("ioutil.ReadAll, error %v", err), http.StatusBadRequest)
			return
		}

		w.Header().Set("My-custom-header", "my-header-data")
		w.WriteHeader(http.StatusOK)
		w.Write(data)

	}()
	select {
	case <-time.After(RequestMaxWaitTimeInterval):
		tr.CancelRequest(req)
		log.Printf("Request timeout")
		<-c // Wait for goroutine to return.
		return
	case <-closeNotify:
		tr.CancelRequest(req)
		log.Printf("Request canceled")
		<-c // Wait for goroutine to return.
		return
	case err := <-c:
		if err!=nil{
			log.Printf("Error in request goroutine %v",err)
		}
		return
	}
}
```


## #2 - Import packages
> 2016-26-01 by [@beyondns](https://github.com/beyondns)

```go
import "fmt"    // fmt.Print()
import ft "fmt" // ft.Print()
import . "fmt"  // Print()
import _ "fmt"  // not use, but run init
```

## #1 - Map
> 2016-26-01

Map is a hash table
```go
// Set/Get/Test 
m[k]=v
v=m[k]
if _,ok:=m[k];ok{
	// m[k] exists
} 

// Iterate over map, random order
for k,v:=range m{
	// k - key
	// v - value
}

// Function argument
type M map[TK]TV
func f(m M) // m - passed by value, but memory the same 
func f(m *M)// m - passed by reference, but memory the same 

// Double, triple,... map
m := make(map[TK1]map[TK2]TV)
m[K1]=make(map[TK2]TV)
m[K1][K2]=V

// Implemet set with empty struct 
s := make(map[TK]struct{})
s[K]=struct{}{} // set K
delete(s,K)     // unset K
```
* [go-maps-in-action](https://blog.golang.org/go-maps-in-action)

## #0 - Slices
> 2016-24-01 by [@beyondns](https://github.com/beyondns)

Slice is a dynamic array
```golang

// Iterate over slice
for i,v:=range s{ // use range, inc order
	// i - index
	// v - value
} 
for i:=0;i<len(s);i++{ // use index, inc order
	// i - index
	// s[i] - value
} 
for i:=len(s)-1;i>=0;i--{ // use index, reverse order
	// i - index
	// s[i] - value
} 


// Function argument
func f(s []T) // s - passed by value, but memory the same 
func f(s *[]T) // s - passed by refernce, but memory the same 

// Append
a = append(a, b...)

// Clone
b = make([]T, len(a))
copy(b,a)

// Remove element, keep order
a = a[:i+copy(a[i:], a[i+1:])]
// or
a = append(a[:i], a[i+1:]...)

// Remove element, change order
a[i] = a[len(a)-1] 
a = a[:len(a)-1]

```
* [go-slices-usage-and-internals](https://blog.golang.org/go-slices-usage-and-internals)
* [SliceTricks](https://github.com/golang/go/wiki/SliceTricks)
