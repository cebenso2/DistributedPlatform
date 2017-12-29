package main

import (
	"encoding/json"
	//"fmt"
	"io/ioutil"
	"os"
	"strconv"
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

const NumVertices = 334863

//Modify code here for different functions
//===============================================================
func compute(vs VertexState) ComputeOutput {
	var sum float64 = 0
	for _, v := range vs.IncomingMessages {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			//fmt.Println("float parse failed")
		}
		sum += f
	}
	value := -1.0
	if SuperStep == 0 {
		value = 1.0 / 334863
	} else {
		value = 0.15/334863 + 0.85*sum
	}
	//value := 0.15/NumVertices + 0.85*sum
	vs.Value = strconv.FormatFloat(value, 'f', -1, 64)
	vs.IncomingMessages = nil
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
