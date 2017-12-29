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
	Value      string `json:"value"`
	DestVertex string `json:"dv"`
}

type ComputeOutput struct {
	Vs       VertexState     `json:"vs"`
	Messages []VertexMessage `json:"messages"`
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
	var vs VertexState
	var result ComputeOutput
	//var thing string
	if err := json.NewDecoder(os.Stdin).Decode(&vs); err != nil {
		//fmt.Println(err)
	}
	vs.Value = "0.1"
	result = compute(vs)
	//fmt.Println(result.Messages)
	enc := json.NewEncoder(os.Stdout)
	//vs.Value = result.VertexState.Value
	//vs = result.VertexState
	enc.Encode(result)
	//enc.Encode(result.VertexState)
	//enc.Encode(result.Messages)
}

//XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxxxxx
