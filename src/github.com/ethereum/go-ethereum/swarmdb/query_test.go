package swarmdb_test

import (
	//"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/swarmdb"
	"testing"
)

func TestParseQuery(t *testing.T) {

	/*
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
		err := swarmdb.CreateTable(treetype, table, index, btreetestcols)
		if err != nil {
			t.Fatal(err)
		}

		for key, rec := range records {
			err := swarmdb.AddRecord(owner, table, key, rec)
			if err != nil {
				t.Fatal(err)
			}
		}
	*/

	rawqueries := map[string]string{
		`precedence`:   `select * from a where a=b and c=d or e=f`,
		`operator`:     `select name, age from contacts where age >= 35`,
		`like`:         `select name, age from contacts where email like '%wolk%'`,
		`is`:           `select name, age from contacts where age is not null`,
		`get`:          `select name, age from contacts where email = 'rodney@wolk.com'`,
		`doublequotes`: `select name, age from contacts where email = "rodney@wolk.com"`,
		`and`:          `select name, age from contacts where email = 'rodney@wolk.com' and age = 38`,
		`or`:           `select name, age from contacts where email = 'rodney@wolk.com' or age = 35`,
		`not`:          `select name, age from contacts where email != 'rodney@wolk.com'`,
		`groupby`:      `select name, age from contacts where age >= 35 group by email`,
		`syntax`:       `select age from test where email = 'test002@wolk.com'`,
	}
	/*
		test := `{"Type":"Select", "Table":"contacts", "TableOwner":, "RequestColumns":[{"ColumnName":"name", "IndexType":0, "ColumnType":0, "Primary":0}, {"ColumnName":"age", "IndexType":0, "ColumnType":0, "Primary":0}], "Where":{"Left":"email", "Right":"%wolk%", "Operator":"like"}, "Ascending":0}`
		var q swarmdb.QueryOption
		_ = json.Unmarshal([]byte(test), q)

		fmt.Printf("q is:\n%+v\n", q)
	*/

	for testid, raw := range rawqueries {
		fmt.Printf("[%s] raw: %s\n", testid, raw)
		clean, err := swarmdb.ParseQuery(raw)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Printf("clean: %+v\n\n", clean)
	}

	/*
		expected := map[string][]string{
			`get`: []string{`{"name": "Rodney", "age": 38}`},
			`and`: []string{`{"name": "Rodney", "age": 38}`},
			`or`:  []string{`{"name": "Rodney", "age": 38}`, `{"name": "Alina", "age": 35}`},
			`not`: []string{`{"name": "Alina", "age": 35}`, `{"name": "Who", "age": 38}`},
		}

		for prefix, q := range queries {

			fmt.Printf("Query test: %s: %s\n", prefix, q)
			actual, err := swarmdb.Query(owner, table, q)
			if err != nil {
				t.Fatal(err)
			}

			//compare output slices (may be in a different order than 'expected', also values maybe in a different order.)

			if len(expected[prefix]) != len(actual) {
				t.Fatal(fmt.Errorf("expected and actual are not the same.\nexpected: %v\nactual: %v\n", expected[prefix], actual))
			}

			for _, exp := range expected[prefix] {

				expmap := make(map[string]string)
				if err := json.Unmarshal([]byte(exp), &expmap); err != nil {
					t.Fatal(err)
				}

				found := false
				for _, act := range actual {

					actmap := make(map[string]string)
					if err := json.Unmarshal([]byte(act), &actmap); err != nil {
						t.Fatal(err)
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
					t.Fatal(fmt.Errorf("%s test. actual: %v, expected %v", prefix, actual, expected[prefix]))
				}

			}

			fmt.Printf("success. %+v\n", actual)

		}
	*/
}
