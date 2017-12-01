package swarmdb

import (
	"fmt"
	"testing"
)

func TestCreateTable(t *testing.T) {

	tests := map[string][]string{
		"hash":  []string{"contacts", "email", "true", "hash"},
		"kad":   []string{"pizzarias", "phone", "true", "kademlia"},
		"btree": []string{"movies", "imdb", "true", "btree"},
	}
	for _, test := range tests {
		err := CreateTable(test[0], test[1], true, test[3])
		if err != nil {
			t.Fatal(err)
		}
	}

}

func TestAddRecord(t *testing.T) {

	var records []string
	records = append(records, `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`)
	records = append(records, `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`)
	records = append(records, `{ "email": "who@wolk.com", "name": "Who", "age": 38 }`)
	records = append(records, `{ "email": "who@wolk.com", "name": "Who", "age": 38 }`)

	err := CreateTable("contacts", "email", true, "hash")
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range records {
		err := AddRecord("contacts", r)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestGetRecord(t *testing.T) {

	var records []string
	records = append(records, `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`)
	records = append(records, `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`)
	records = append(records, `{ "email": "who@wolk.com", "name": "Who", "age": 38 }`)

	err := CreateTable("contacts", "email", true, "hash")
	if err != nil {
		t.Fatal(err)
	}

	for _, r := range records {
		err := AddRecord("contacts", r)
		if err != nil {
			t.Fatal(err)
		}
	}

	tests := map[string][]string{
		"ok":    []string{"contacts", "rodney@wolk.com"},
		"notok": []string{"contacts", "wobble@gmail.com"},
	}

	expected := map[string][]string{
		"ok":    []string{`{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`},
		"notok": []string{},
	}

	for prefix, param := range tests {
		actual, err := GetRecord(param[0], param[1])
		if err != nil {
			t.Fatal(err)
		}
		//compare output slices - get ans should only be 1 record
		if actual != expected[prefix][0] {
			t.Fatal(fmt.Errorf("%s test. actual: %v, expected %v", prefix, actual, expected[prefix][0]))
		}
	}
}

func TestQuery(t *testing.T) {

	err := CreateTable("contacts", "email", true, "hash") //need one for kad, btree
	if err != nil {
		t.Fatal(err)
	}

	var records []string
	records = append(records, `{ "email": "rodney@wolk.com", "name": "Rodney", "age": 38 }`)
	records = append(records, `{ "email": "alina@wolk.com", "name": "Alina", "age": 35 }`)
	records = append(records, `{ "email": "who@wolk.com", "name": "Who", "age": 38 }`)

	for _, r := range records {
		err := AddRecord("contacts", r)
		if err != nil {
			t.Fatal(err)
		}
	}

	queries := map[string]string{
		``:    `select name, age from contacts where email = 'rodney@wolk.com'`,
		`and`: `select name, age from contacts where email = 'rodney@wolk.com' and age = 38`,
		`or`:  `select name, age from contacts where email = 'rodney@wolk.com' or age = 35`,
	}

	expected := map[string][]string{
		``:    []string{`{"name": "Rodney", "age": 38}`},
		`and`: []string{`{"name": "Rodney", "age": 38}`},
		`or`:  []string{`{"name": "Rodney", "age": 38}`, `{"name": "Alina", "age": 35}`},
	}

	for prefix, q := range queries {
		actual, err := Query(q)
		if err != nil {
			t.Fatal(err)
		}
		//compare output slices (may be in a different order than 'expected')
		for _, exp := range expected[prefix] {
			found := false
			for _, act := range actual {
				if act == exp {
					found = true
					break
				}
			}
			if found == false {
				t.Fatal(fmt.Errorf("%s test. actual: %v, expected %v", prefix, actual, expected[prefix]))
			}
		}
	}

}
