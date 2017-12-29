package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type VertexState struct {
	Value            string
	OutVertices      []string
	IncomingMessages []string
	Active           bool
}

type VertexMessage struct {
	Value      string
	DestVertex string
}

type ComputeOutput struct {
	VertexState VertexState
	Messages    []VertexMessage
}

//Modify code here for different functions
//===============================================================
func compute(vs VertexState) ComputeOutput {
	//TODO Compute function
	vs.IncomingMessages = []string{}
	messages := []VertexMessage{}
	return ComputeOutput{vs, messages}
}

//===============================================================

//Template for EXE file do not change
//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
func runCompute(vsFile string) ComputeOutput {
	jsonVS, _ := ioutil.ReadFile(vsFile)
	var vs VertexState
	json.Unmarshal(jsonVS, &vs)
	return compute(vs)
}

func main() {
	if len(os.Args) != 2 {
		fmt.Println("go run <exe file>.go <vertex file>.json")
		return
	}
	result := runCompute(os.Args[1])
	thing, _ := json.Marshal(result)
	json.Unmarshal(thing, &result)
	fmt.Println(result.VertexState.Value)
	fmt.Print(result)
	f, err := os.Create("result.json")
	if err != nil {
		fmt.Println(err)
	}
	f.Write([]byte(thing))
	f.Close()
}

//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxxxx
