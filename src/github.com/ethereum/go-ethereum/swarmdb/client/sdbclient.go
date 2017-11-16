package client

import (
	"bytes"
	//"encoding/json"
	//"errors"
	"fmt"
	//"io"
	"io/ioutil"
	//"mime"
	//"mime/multipart"
	"net/http"
	//"net/textproto"
	//"os"
	//"path/filepath"
	//"strconv"
	//"strings"

)

var (
	DefaultGateway = "http://localhost:8500"
	DefaultClient  = NewClient(DefaultGateway)
)

func NewClient(gateway string) *Client {
	return &Client{
		Gateway: gateway,
	}
}

// Client wraps interaction with a swarm HTTP gateway.
type Client struct {
	Gateway string
}

func CreateTable(tbl_name, column, index string, primary bool)(error){
	client := NewClient(DefaultGateway)
	indextype := "KD"
	if index == "hash"{
		indextype = "HD"
	}
	err := client.createTable(tbl_name, column, indextype, primary)
	return err
}
func (c *Client)createTable(tbl_name, column, indextype string, primary bool)(error){
	pri := "0"
	if primary{
		pri = "1"
	}
	uri := c.Gateway + "/bzzr:/tabledata/" + tbl_name
	res, err := http.DefaultClient.Get(uri)
	fmt.Println(uri)
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		res.Body.Close()
		return fmt.Errorf("unexpected HTTP status: %s", res.Status)
	}

	buf := make([]byte, 4096)
	for i := 0; i < 1; i++{
		copy(buf[i*64+2048:], column)
		copy(buf[i*64+2048+28:], pri)
		copy(buf[i*642048+28+1:], indextype)
	}
	req, err := http.NewRequest("POST", c.Gateway+"/bzzr:/", bytes.NewReader(buf))
        if err != nil {
                return err
        }
	req.ContentLength = int64(len(buf))
	res, err = http.DefaultClient.Do(req)

	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected HTTP status: %s", res.Status)
	}
	data, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return err
	}
/*
    	tabledata := map[string]string{"tablename": table, "key": string(data)}
    	tablejson, _ := json.Marshal(tabledata)
*/
	
	fmt.Printf("data = %s %d\n",data, len(data))
	a :=  bytes.NewReader(data)
	b, _ :=ioutil.ReadAll(a)
	fmt.Printf("a, b = %s\n", b)
        req2, err := http.NewRequest("POST", c.Gateway+"/bzzr:/tabledata/"+tbl_name, bytes.NewReader([]byte(data)))
	res2, err := http.DefaultClient.Do(req2)
        if res2.StatusCode != http.StatusOK {
                res.Body.Close()
                return fmt.Errorf("unexpected HTTP status: %s", res2.Status)
        }


	return nil
}

/*
func main(){
	cl := NewClient(DefaultGateway)
	fmt.Println(cl)
	str, err := cl.CreateTable()
	fmt.Println(str, err)
}
*/
