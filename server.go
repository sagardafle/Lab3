package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/julienschmidt/httprouter"
)

type KeyValuePairStruct struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

var instance1, instance2, instance3 []KeyValuePairStruct
var temp1, temp2, temp3 int

type ByKey []KeyValuePairStruct

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func GetAllPairs(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	portNumber := strings.Split(request.Host, ":") // to get the port number from URL
	if portNumber[1] == "3000" {
		//add the key-value to instance with port 3000
		sort.Sort(ByKey(instance1))
		output, _ := json.Marshal(instance1)
		fmt.Fprintln(rw, string(output))
	} else if portNumber[1] == "3001" {
		//add the key-value to instance with port 3001
		sort.Sort(ByKey(instance2))
		output, _ := json.Marshal(instance2)
		fmt.Fprintln(rw, string(output))
	} else {
		//add the key-value to instance with port 3002
		sort.Sort(ByKey(instance3))
		output, _ := json.Marshal(instance3)
		fmt.Fprintln(rw, string(output))
	}
}

func PutPair(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	portNumber := strings.Split(request.Host, ":")
	key, _ := strconv.Atoi(p.ByName("key_id"))
	if portNumber[1] == "3000" {
		instance1 = append(instance1, KeyValuePairStruct{key, p.ByName("value")})
		temp1++
	} else if portNumber[1] == "3001" {
		instance2 = append(instance2, KeyValuePairStruct{key, p.ByName("value")})
		temp2++
	} else {
		instance3 = append(instance3, KeyValuePairStruct{key, p.ByName("value")})
		temp3++
	}
}

func GetSinglePair(rw http.ResponseWriter, request *http.Request, p httprouter.Params) {
	result := instance1
	ind := temp1
	portNumber := strings.Split(request.Host, ":")
	if portNumber[1] == "3001" {
		result = instance2
		ind = temp2
	} else if portNumber[1] == "3002" {
		result = instance3
		ind = temp3
	}
	key, _ := strconv.Atoi(p.ByName("key_id"))
	for i := 0; i < ind; i++ {
		if result[i].Key == key {
			output, _ := json.Marshal(result[i])
			fmt.Fprintln(rw, string(output))
		}
	}
}

func main() {
	temp1 = 0
	temp2 = 0
	temp3 = 0
	mux := httprouter.New()
	mux.PUT("/keys/:key_id/:value", PutPair) // Will call the PutPAir function to add the pair values .
	mux.GET("/keys/:key_id", GetSinglePair)  // Will call the GetSinglePair function to return a single pair based on search value.
	mux.GET("/keys", GetAllPairs)            // Will call the GetAllPairs function to return all the pairs pertaining to the specified instance.

	go http.ListenAndServe(":3000", mux) //Listener for 3000 port
	go http.ListenAndServe(":3001", mux) //Listener for 3001 port
	go http.ListenAndServe(":3002", mux) //Listener for 3002 port
	select {}
}
