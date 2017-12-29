package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"math"
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

const SourceVertex = "10"
//Modify code here for different functions
//===============================================================
func compute(vs VertexState) ComputeOutput {
	var minDist float64
	if vs.Value == SourceVertex {
		minDist = 0
	} else {
		minDist = 100000000
	}
	for _, v := range vs.IncomingMessages {
		dist, err := strconv.ParseFloat(v, 64)
		if err !=nil {
			fmt.Println("float parse failed")
		}
		minDist = math.Min(minDist, dist)
	}
	//assuming graph is edges are length 1
	vs.Value = strconv.FormatFloat(minDist + 1.0 , 'f', -1, 64)
	vs.IncomingMessages = []string{}
	messages := []VertexMessage{}
	for _, id := range vs.OutVertices {
		messages = append(messages, VertexMessage{id, vs.Value})
	}
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
	fmt.Print(result);
	f, err := os.Create("result.json")
	if err != nil {
		fmt.Println(err)
	}
	f.Write([]byte(thing))
	f.Close()
}

//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxxxx
