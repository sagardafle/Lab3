package main

import (
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
)

type CircleHashArray []uint32

type Nodes struct {
	Id int
	IP string
}

type KeyValuePair struct {
	Key   int    `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

func NewNode(id int, ip string) *Nodes {
	return &Nodes{
		Id: id,
		IP: ip,
	}
}
func ConsistentHashing() *ConsistentHashingStruct {
	return &ConsistentHashingStruct{
		Nodes:     make(map[uint32]Nodes),
		IsPresent: make(map[int]bool),
		Circle:    CircleHashArray{},
	}
}

func (cha CircleHashArray) Len() int {
	return len(cha)
}

func (cha CircleHashArray) Less(i, j int) bool {
	return cha[i] < cha[j]
}

func (cha CircleHashArray) Swap(i, j int) {
	cha[i], cha[j] = cha[j], cha[i]
}

type ConsistentHashingStruct struct {
	Nodes     map[uint32]Nodes
	IsPresent map[int]bool
	Circle    CircleHashArray
}

func (cha *ConsistentHashingStruct) AddNodeToCircle(node *Nodes) bool {

	if _, ok := cha.IsPresent[node.Id]; ok {
		return false
	}
	str := cha.ReturnNodeAddress(node)
	cha.Nodes[cha.GetHashValue(str)] = *(node)
	cha.IsPresent[node.Id] = true
	cha.SortHashCircle()
	return true
}

func (cha *ConsistentHashingStruct) SortHashCircle() {
	cha.Circle = CircleHashArray{}
	for k := range cha.Nodes {
		cha.Circle = append(cha.Circle, k)
	}
	sort.Sort(cha.Circle)
}

func (cha *ConsistentHashingStruct) GetHashValue(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (cha *ConsistentHashingStruct) ReturnNodeAddress(node *Nodes) string {
	return node.IP
}

func (cha *ConsistentHashingStruct) FindNode(hash uint32) int {
	k := sort.Search(
		len(cha.Circle),
		func(k int) bool {
			return cha.Circle[k] >= hash
		})
	if k < len(cha.Circle) {
		if k == len(cha.Circle)-1 {
			return 0
		} else {
			return k
		}
	} else {
		return len(cha.Circle) - 1
	}
}

func (cha *ConsistentHashingStruct) Get(key string) Nodes {
	hash := cha.GetHashValue(key)
	i := cha.FindNode(hash)
	return cha.Nodes[cha.Circle[i]]
}

func PutPair(nodeCircle *ConsistentHashingStruct, str string, input string) {

	ipAddress := nodeCircle.Get(str)
	address := "http://" + ipAddress.IP + "/keys/" + str + "/" + input
	fmt.Println("address=============", address)
	req, err := http.NewRequest("PUT", address, nil)
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer resp.Body.Close()
		fmt.Println("PUT Request successfully completed")
	}
}

func GetSinglePair(key string, nodeCircle *ConsistentHashingStruct) {
	var out KeyValuePair
	ipAddress := nodeCircle.Get(key)
	address := "http://" + ipAddress.IP + "/keys/" + key
	fmt.Println(address)
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}

func GetAllPairs(address string) {

	var out []KeyValuePair
	response, err := http.Get(address)
	if err != nil {
		fmt.Println("Error:", err)
	} else {
		defer response.Body.Close()
		contents, err := ioutil.ReadAll(response.Body)
		if err != nil {
			fmt.Println(err)
		}
		json.Unmarshal(contents, &out)
		result, _ := json.Marshal(out)
		fmt.Println(string(result))
	}
}

func takeInputFromUser() (str string, input string) {
	fmt.Println("Enter the Key:")
	fmt.Scanf("%s\n", &str)
	fmt.Println("Enter the value:")
	fmt.Scanf("%s\n", &input)
	return str, input
}

func main() {
	nodeCircle := ConsistentHashing()
	var addtonode Nodes
	noOfInstances := 3

	var instanceArray [3]string
	instanceArray[0] = "127.0.0.1:3000"
	instanceArray[1] = "127.0.0.1:3001"
	instanceArray[2] = "127.0.0.1:3002"

	for i := 0; i < noOfInstances; i++ {
		addtonode = *NewNode(i, instanceArray[i])
		nodeCircle.AddNodeToCircle(&addtonode)
	}

	pairLimit := 10
	fmt.Println("You will be prompted to enter the key-value pair for ", pairLimit, "times")

	for i := 1; i <= pairLimit; i++ {
		userKey, userValue := takeInputFromUser()
		PutPair(nodeCircle, userKey, userValue)
	}

	fmt.Println("======Printing GET=========")

	for i := 1; i <= pairLimit; i++ {
		index := strconv.Itoa(i)
		GetSinglePair(index, nodeCircle)
	}

	var getallStrings [3]string

	getallStrings[0] = "http://127.0.0.1:3000/keys"
	getallStrings[1] = "http://127.0.0.1:3001/keys"
	getallStrings[2] = "http://127.0.0.1:3002/keys"

	fmt.Println("=========Printing GETALL=========")

	for i := 0; i < len(getallStrings); i++ {
		GetAllPairs(getallStrings[i])
	}
}
