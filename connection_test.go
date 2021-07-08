package impalathing

import (
	"fmt"
	"log"
	"testing"
	"time"
)

func Test_Connection(t *testing.T) {
	host := "impala-host"
	port := 21000

	//con, err := Connect(host, port)
	// if you use kerberos
	con, err := Connect(host, port, WithGSSAPISaslTransport())
	if err != nil {
		log.Fatal("Error connecting", err)
		return
	}

	query, err := con.Query("SELECT user_id, action, yyyymm FROM engagements LIMIT 10000")

	startTime := time.Now()
	total := 0
	for query.Next() {
		var (
			user_id string
			action  string
			yyyymm  int
		)

		query.Scan(&user_id, &action, &yyyymm)
		total += 1

		fmt.Println(user_id, action)
	}

	log.Printf("Fetch %d rows(s) in %.2fs", total, time.Duration(time.Since(startTime)).Seconds())
}
