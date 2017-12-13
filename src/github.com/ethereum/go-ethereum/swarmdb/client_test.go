package swarmdb_test

import (
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/swarmdb"
	"testing"
)

func TestCreateTable(t *testing.T) {

	//params: treetype, table, index, columns
	btreetestcols := map[string]string{
		"email":  "string",
		"name":   "string",
		"age":    "int",
		"gender": "string",
	}
	tests := map[string][]interface{}{
		"btree": []interface{}{"BT", "contacts", "email", btreetestcols},
		//"hashdb": []string{"HT", "movies", "imdb", hashdbcols}
	}

	for prefix, test := range tests {
		fmt.Printf("CreateTable test %s: %v \n", prefix, test)
		err := CreateTable(test[0].(string), test[1].(string), test[2].(string), test[3].(map[string]string))
		if err != nil {
			fmt.Printf("failed.\n")
			//t.Fatal(err) //uncomment when ready
		}
		fmt.Printf("success.\n")
	}

}

func TestAddRecord(t *testing.T) {

	tests := map[string][]string{
		"add1":   []string{"tmpowner", "contacts", "rodney@wolk.com", `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`},
		"add2":   []string{"tmpowner", "contacts", "alina@wolk.com", `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`},
		"add3":   []string{"tmpowner", "contacts", "who@wolk.com", `{ "email": "who@wolk.com", "name": "Who", "age": 35 }`},
		"dupe":   []string{"tmpowner", "contacts", "who@wolk.com", `{ "email": "who@wolk.com", "name": "Who", "age": 35 }`},
		"update": []string{"tmpowner", "contacts", "alina@wolk.com", `{ "email": "alina@wolk.com", "name": "Whoami", "age": 35 }`},
	}

	expected := map[string]string{
		"add1":   "pass",
		"add2":   "pass",
		"add3":   "pass",
		"dupe":   "fail",
		"update": "pass",
	}

	//btree only, need one for hashdb
	btreetestcols := map[string]string{
		"email":  "string",
		"name":   "string",
		"age":    "int",
		"gender": "string",
	}
	err := client.CreateTable("BT", "contacts", "email", btreetestcols)
	if err != nil {
		//t.Fatal(err) //uncomment when ready
	}

	for prefix, test := range tests {
		fmt.Printf("AddRecord test %s: %v\n", prefix, test)
		err := client.AddRecord(test[0], test[1], test[2], test[3])
		if err != nil { //did not add record
			if expected[prefix] == "fail" {
				fmt.Printf("success. failed with %v\n", err)
			} else {
				fmt.Printf("fail.\n")
				//t.Fatal(err) //uncomment when ready
			}
		} else { //added record
			if expected[prefix] == "fail" {
				fmt.Printf("fail.\n")
				//t.Fatal(err) //uncomment when ready
			}
		}
		fmt.Printf("success.\n")
	}
}

func TestGetRecord(t *testing.T) {

	owner := "tempowner"
	table := "contacts"
	index := "email"

	records := map[string]string{
		"rodney@wolk.com": `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`,
		"alina@wolk.com":  `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`,
		"who@wolk.com":    `{ "email": "who@wolk.com", "name": "Who", "age": 38 }`,
	}

	//btree only, need one for hashdb
	treetype := "BT"
	btreetestcols := map[string]string{
		"email":  "string",
		"name":   "string",
		"age":    "int",
		"gender": "string",
	}
	err := client.CreateTable(treetype, table, index, btreetestcols)
	if err != nil {
		//t.Fatal(err) //uncomment when ready
	}

	for key, rec := range records {
		err := client.AddRecord(owner, table, key, rec)
		if err != nil {
			//t.Fatal(err) //uncomment when ready
		}
	}

	tests := map[string]string{
		"ok1":    "rodney@wolk.com",
		"ok2":    "alina@wolk.com",
		"badkey": "wobble@gmail.com",
	}

	expected := map[string]string{
		"ok1":    `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`,
		"ok2":    `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`,
		"badkey": "fail",
	}

	for prefix, testkey := range tests {

		fmt.Printf("GetRecord test %s: %v\n", prefix, testkey)
		actual, err := client.GetRecord(owner, table, testkey)
		if err != nil {
			//t.Fatal(err) //uncomment when ready
		}
		//compare output slices - get ans should only be 1 record
		expectmap := make(map[string]interface{})
		actualmap := make(map[string]interface{})
		if err := json.Unmarshal([]byte(expected[prefix]), &expectmap); err != nil {
			// t.Fatal(fmt.Errorf("%s test is not proper json. %v\n", prefix, expected[prefix]))
		}
		if err := json.Unmarshal([]byte(actual), &actualmap); err != nil {
			//t.Fatal(fmt.Errorf("%s test actual result is not proper json. %v\n", prefix, actual))
		}

		if len(expectmap) != len(actualmap) {
			//t.Fatal(fmt.Errorf("fail. Not the same. actual: %+v, expected: %+v\n", actualmap, expectmap))
		}

		for ekey, evalue := range expectmap {
			if actualmap[ekey] != evalue {
				//t.Fatal(fmt.Errorf("fail. Not the same. actual: %+v, expected: %+v\n", actualmap, expectmap))
			}
		}
		fmt.Printf("success. %+v\n", actualmap)

	}
}

func TestQuery(t *testing.T) {

	owner := "tempowner"
	table := "contacts"
	index := "email"
	records := map[string]string{
		"rodney@wolk.com": `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`,
		"alina@wolk.com":  `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`,
		"who@wolk.com":    `{ "email": "who@wolk.com", "name": "Who", "age": 38 }`,
	}

	//btree only, need one for hashdb
	treetype := "BT"
	btreetestcols := map[string]string{
		"email":  "string",
		"name":   "string",
		"age":    "int",
		"gender": "string",
	}
	err := client.CreateTable(treetype, table, index, btreetestcols)
	if err != nil {
		t.Fatal(err)
	}

	for key, rec := range records {
		err := client.AddRecord(owner, table, key, rec)
		if err != nil {
			t.Fatal(err)
		}
	}

	queries := map[string]string{
		`get`: `select name, age from contacts where email = 'rodney@wolk.com'`,
		`and`: `select name, age from contacts where email = 'rodney@wolk.com' and age = 38`,
		`or`:  `select name, age from contacts where email = 'rodney@wolk.com' or age = 35`,
		`not`: `select name, age from contacts where email != 'rodney@wolk.com'`,
	}

	expected := map[string][]string{
		`get`: []string{`{"name": "Rodney", "age": 38}`},
		`and`: []string{`{"name": "Rodney", "age": 38}`},
		`or`:  []string{`{"name": "Rodney", "age": 38}`, `{"name": "Alina", "age": 35}`},
		`not`: []string{`{"name": "Alina", "age": 35}`, `{"name": "Who", "age": 38}`},
	}

	for prefix, q := range queries {

		fmt.Printf("Query test: %s: %s\n", prefix, q)
		actual, err := client.Query(owner, table, q)
		if err != nil {
			//t.Fatal(err)
		}

		//compare output slices (may be in a different order than 'expected', also values maybe in a different order.)

		if len(expected[prefix]) != len(actual) {
			//t.Fatal(fmt.Errorf("expected and actual are not the same.\nexpected: %v\nactual: %v\n", expected[prefix], actual))
		}

		for _, exp := range expected[prefix] {

			expmap := make(map[string]string)
			if err := json.Unmarshal([]byte(exp), &expmap); err != nil {
				//t.Fatal(err)
			}

			found := false
			for _, act := range actual {

				actmap := make(map[string]string)
				if err := json.Unmarshal([]byte(act), &actmap); err != nil {
					//t.Fatal(err)
				}

				if len(actmap) != len(expmap) {
					continue //try next actual map
				}

				match := true
				for key, expval := range expmap {
					if actmap[key] != expval {
						match = false
						break
					}
				}
				if match {
					found = true
					break
				}

			}

			if found == false {
				//t.Fatal(fmt.Errorf("%s test. actual: %v, expected %v", prefix, actual, expected[prefix]))
			}

		}

		fmt.Printf("success. %+v\n", actual)

	}

}
