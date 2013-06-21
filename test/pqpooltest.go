package main

/*
	Performance test module for lib/pq connection pooling

    Run this test with maxcons and persist options:

		PGHOST=localhost PGPORT=5432 PGDATABASE=testdb PGUSER=testuser PGPASSWORD=testpass \
		TESTLOOPS=1000 PGMAXCONS=10 PGPERSIST=true ./pqpooltest
*/

import (
	_ "github.com/ld9999999999/pqpooling"
	"database/sql"
	"fmt"
	"time"
	"os"
	"strconv"
)

func main() {
	LOOPS := 1000
	loops := os.Getenv("TESTLOOPS")
	if loops != "" {
		n, err := strconv.Atoi(loops)
		if err != nil {
			fmt.Println("TESTLOOPS not valid")
			return
		}
		LOOPS = n
	}

	db, err := sql.Open("postgres", "")
	if err != nil {
		fmt.Println("ErrOpen:", err)
		return
	}
	defer db.Close()

	mcopt := os.Getenv("PGMAXCONS")
	if mcopt != "" {
		db.SetMaxIdleConns(0)
	}

	_, err = db.Exec("CREATE TABLE pqpooltest(user_name TEXT, first_name TEXT, last_name TEXT)")
	if err != nil {
		fmt.Println("Error create temp table:", err)
		return
	}else {
		fmt.Println("OK")
	}

	done := make(chan int)

	fmt.Println("Running test with", LOOPS, "goroutines")

	t1 := time.Now()
	for x := 0; x < LOOPS; x++ {
		go func(i int) {
			if i % 2 == 0 {
				_, err := db.Exec("INSERT INTO pqpooltest (user_name, first_name, last_name) VALUES ('alpha', 'beta', 'gamma')")
				if err != nil {
					fmt.Println("Err:", err)
				}
		    } else {
				 rows, err := db.Query("SELECT user_name, first_name, last_name from pqpooltest LIMIT 50")
				if err != nil {
					fmt.Println("Err:", err)
				} else {
			        for rows.Next() {
			            if i == 3 {
			                var user_name string
			                var first_name string
			                var last_name string
			                err = rows.Scan(&user_name, &first_name, &last_name)
			                fmt.Println("....", user_name, first_name, last_name)
			            }
			        }
					rows.Close()
				}
		    }

			done <-1
		}(x)
	}

	for i:=0; i < LOOPS; i++ {
		<-done
	}
	t2 := time.Now()
	_, err = db.Exec("DROP TABLE pqpooltest")

	fmt.Println("TOTAL TIME (ns):", t2.UnixNano() - t1.UnixNano())
}


