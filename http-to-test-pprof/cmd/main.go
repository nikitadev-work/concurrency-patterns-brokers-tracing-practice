// CPU Load

// package main

// import (
// 	"fmt"
// 	"net/http"
// 	_ "net/http/pprof"
// )

// func main() {
// 	go func() {
// 		http.ListenAndServe("localhost:6060", nil)
// 	}()

// 	http.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
// 		total := 0
// 		for i := 0; i < 1000000; i++ {
// 			total += i
// 		}
// 		fmt.Fprintf(w, "done %d\n", total)
// 	})

// 	http.ListenAndServe(":8080", nil)
// }
// --------------------------------------------------------------------------
// Memory Load

package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
)

var sink [][]byte

func main() {
	go func() { _ = http.ListenAndServe("localhost:6060", nil) }()

	mux := http.NewServeMux()

	mux.HandleFunc("/alloc", func(w http.ResponseWriter, r *http.Request) {
		for i := 0; i < 2000; i++ {
			_ = make([]byte, 32*1024)
		}
		fmt.Fprintln(w, "ok alloc")
	})

	mux.HandleFunc("/leak", func(w http.ResponseWriter, r *http.Request) {
		chunk := make([]byte, 50*1024*1024) // 50MB
		sink = append(sink, chunk)
		fmt.Fprintf(w, "ok leak; chunks=%d; approx=%dMB\n", len(sink), len(sink)*50)
	})

	_ = http.ListenAndServe(":8080", mux)
}

