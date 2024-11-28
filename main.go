package main

import (
	"log"
	"os"

	"github.com/hasssanezzz/goldb-engine/engine"
)

func gen() {
	// mp := memtable.New()
	// n := 1_000
	// for i := 0; i < n; i++ {
	// 	mp.Set(fmt.Sprintf("key-%d", i), avl.IndexNode{
	// 		Offset: uint32(rand.Intn(n * 2)),
	// 		Size:   uint32(i),
	// 	})
	// }
	// mp.Set("key-55", memtable.IndexNode{Size: 69})

	// mngr, err := index_manager.New("./.db")
	// if err != nil {
	// 	panic(err)
	// }

	// mngr.Flush()

	// r, err := mngr.Read("key-55")
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println(r)

	// file, err := os.Create("sst_1.bin")
	// if err != nil {
	// 	panic(err)
	// }
	// defer file.Close()
	// index_manager.Flush(mp, file, 1)
}

// func parse() {
// 	err := index_manager.NewSSTable("sst_1.bin", 1)
// 	defer st.Close()

// 	fmt.Println(st.Serial, st.Size, st.MinKey, st.MaxKey)

// 	result, err := st.BSearch("key-55")
// 	if err != nil {
// 		panic(err)
// 	}
// 	fmt.Println(result)
// }

func main() {
	engine, err := engine.New("./.db")
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	args := os.Args
	if len(args) == 4 || len(args) == 3 {
		cmnd, key := args[1], args[2]
		if cmnd == "get" {
			data, err := engine.Get(key)
			if err != nil {
				panic(err)
			}
			println(string(data))
		}

		if cmnd == "del" {
			engine.Delete(key)
		}

		if cmnd == "set" {
			err := engine.Set(key, []byte(args[3]))
			if err != nil {
				panic(err)
			}
		}
	}

	// err = engine.Set("email", []byte("dhassanezz98@gmail.com"))
	// if err != nil {
	// 	panic(err)
	// }

	// data, err := engine.Get("email")
	// if err != nil {
	// 	log.Println(err)
	// }
	// fmt.Println(string(data))

	// engine.Delete("email")

	// data, err := engine.Get("email")
	// if err != nil {
	// 	log.Println(err)
	// }
	// fmt.Println(string(data))

	// err = engine.Set("name", []byte("Hassan Ezz"))
	// if err != nil {
	// 	panic(err)
	// }
	// err = engine.Set("age", []byte("18"))
	// if err != nil {
	// 	panic(err)
	// }

}
