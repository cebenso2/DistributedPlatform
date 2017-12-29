package main

import (
	"./utils"
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	//"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const HeartbeatCount = 4
const LogFileName = "machine.log"
const FileCopiesNumber = 4
const TimeFormat = "2006-01-02 15:04:05.999999999 -0700 MST"
const timeOutTime = 5
const VertexFilename = "VertexFile.json"
const ResultFilename = "result.json"

type Socket int

var counter = 0
var localfilename = ""
var putRequestTime = ""
var memList []id
var masterData map[string]FileData
var localSDFSData map[string]string
var RepairInprogess = false
var TotalSuperStep = 20

//mp4 global variable
//----------------------------------------------
var LocalVertices map[string]VertexState

//---------------------------------------------

//var graph map[string]VertexState
var isClient = false
var isMaster = false
var isStandbyMaster = false
var SuperStep = 0
var failingFlag = false
var restart = false

type id struct {
	Ip        string
	Timestamp string
}

/*Action is either
  join,
  introducerJoin,
  leave,
  beat,
  fail,
*/
type message struct {
	Action string
	Data   interface{} `json:"data"`
	The_id id
}
type PutRequest struct {
	FileName       string
	PutRequestTime string
}
type File struct {
	FileName       string
	Data           []byte
	PutRequestTime string
}

type WriteLocations struct {
	Locations   []string
	ForceNeeded bool
}

type FileData struct {
	Locations  []string
	LastUpdate string
}

type FileStorageInfo struct {
	FileName string
	Data     FileData
}

type VertexValuePair struct {
	id    string
	value string
}

type InstructMessage struct {
	SuperStep          int
	IncomingMessageMap map[string][]string
}

type InstructMessageFirstTime struct {
	SuperStep int
	VertexMap map[string]VertexState
}

type VertexState struct {
	Value            string
	OutVertices      []string
	IncomingMessages []string
	Active           bool
}

type VertexMessage struct {
	DestVertex string `json:"dv"`
	Value      string `json:"value"`
}

type ComputeOutput struct {
	Vs       VertexState     `json:"vs"`
	Messages []VertexMessage `json:"messages"`
}

type WorkerOutput struct {
	VertexMap  map[string]VertexState
	MessageMap map[string][]string
}

type memberList []id

func (a memberList) Less(i, j int) bool { return a[i].Ip < a[j].Ip }
func (a memberList) Len() int           { return len(a) }
func (a memberList) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func sortList() {
	sort.Sort(memberList(memList))
}

func CheckError(err error) {
	if err != nil {
		fmt.Println("Error: ", err)
		os.Exit(0)
	}
}
func isCurrentMaster(ip string) bool {
	index := getIndexInList(ip)
	if index == 0 {
		return true
	} else {
		return false
	}
}

func getIndexInList(ip string) int {
	for i, item := range memList {
		if item.Ip == ip {
			return i
		}
	}
	return -1
}
func getIndexInFullList(targetIp string) int {
	for i, ip := range utils.ServerIps {
		if ip == targetIp {
			return i
		}
	}
	return -1
}
func deleteFromList(ip string) bool {
	index := 0
	deleteIp := false
	for i, item := range memList {
		if item.Ip == ip {
			index = i
			deleteIp = true
			break
		}
	}
	if deleteIp {
		memList = append(memList[:index], memList[index+1:]...) //delete the indexth item
	}
	return deleteIp
}

func decodeMessage(encodedMessage []byte) message {
	var m message
	err := json.Unmarshal(encodedMessage, &m)
	CheckError(err)
	return m
}
func isIntroducer() bool {
	ip := GetOutboundIP()
	if ip == "172.22.154.22" {
		return true
	} else {
		return false
	}
}

// get local ip address in string
func GetOutboundIP() string {
	conn, _ := net.Dial("udp", "8.8.8.8:80")
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

//create an id for caller
func createMyId(localip string) id {
	curTime := time.Now().String()
	return id{localip, curTime}
}

//create an a beat
func createBeat(localip string) message {
	id := createMyId(localip)
	return message{"beat", "", id}
}

//logs storing event
func logStoringFile(file string) {
	f, err := os.OpenFile("../"+LogFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, err = f.Write([]byte(time.Now().String() + ": storing file in SDFS folder: " + file + "]\n"))
	if err != nil {
		panic(err)
	}
}

//logs storing event
func logStoringFileInCurDir(file string) {
	f, err := os.OpenFile("../"+LogFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, err = f.Write([]byte(time.Now().String() + ": storing file in current directory: " + file + "]\n"))
	if err != nil {
		panic(err)
	}
}

//logs deleting event
func logDeletingFile(file string) {
	f, err := os.OpenFile("../"+LogFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, err = f.Write([]byte(time.Now().String() + ": deleting file in SDFS folder: " + file + "]\n"))
	if err != nil {
		panic(err)
	}
}

func startHeartbeating(quitHeartbeaters <-chan bool, quitFailureListeners <-chan bool, chanMap *map[string]chan bool) {
	startHeartbeatSenders(quitHeartbeaters)
	startHeartbeatListeners(quitFailureListeners, chanMap)
}

func stopHeartbeating(quitHeartbeaters chan<- bool, quitFailureListeners chan<- bool) {
	//fmt.Println("stop hb")
	stopChannels(getNumHeartbeaters(), quitFailureListeners)
	//fmt.Println("done stop listeners")
	stopChannels(getNumHeartbeaters(), quitHeartbeaters)
	//fmt.Println("done stop senders")

}

//send actions to every single server in ips
func broadcast(ips []string, action string, actionId id) []id {
	var list []id
	var wg sync.WaitGroup
	wg.Add(len(ips))

	for i, ip := range ips {
		go func(i int, ip string, actionId id) {
			defer wg.Done()

			hostName := ip
			portNum := "10001"
			service := hostName + ":" + portNum
			RemoteAddr, _ := net.ResolveUDPAddr("udp", service)
			conn, _ := net.DialUDP("udp", nil, RemoteAddr)
			defer conn.Close()

			m := message{
				Action: action,
				Data:   nil,
				The_id: actionId,
			}
			b, _ := json.Marshal(m)

			//send it to the connection
			conn.Write(b)

			//case of Others Joining the group, send them the current Memlist
			if GetOutboundIP() != "172.22.154.22:10001" && ip == "172.22.154.22" && action == "join" {
				// receive message from server
				buffer := make([]byte, 1024)
				n, _, err := conn.ReadFromUDP(buffer)
				CheckError(err)
				err = json.Unmarshal(buffer[0:n], &list)
				CheckError(err)
			}
		}(i, ip, actionId)
	}
	wg.Wait()
	return list
}

func (t *Socket) SendLocation(args *PutRequest, reply *WriteLocations) error {

	log.Println("Sending locations of where to put the file.......")
	fileName := args.FileName
	PutRequestTime := args.PutRequestTime
	//log.Println("fileName:" + fileName)
	//log.Println("putrequestTime:" + PutRequestTime)

	// Todo check whether there is a recent write
	fileData, ok := masterData[fileName]
	var recentUpdate bool = false
	var locations []string
	// fmt.Println("test")
	// fmt.Println(ok)
	if ok { //If file exists; it's an update -> see whether there's a most recent update
		requestTime, _ := time.Parse(TimeFormat, PutRequestTime)
		lastUpdateTime, _ := time.Parse(TimeFormat, fileData.LastUpdate)
		recentUpdate = requestTime.Sub(lastUpdateTime) < 60*1000*time.Millisecond
		fmt.Println(requestTime.Sub(lastUpdateTime))
		fmt.Println(recentUpdate)
		locations = fileData.Locations
	} else { //If file does not exists; it's a post
		for i := counter; i < counter+FileCopiesNumber; i++ {
			locations = append(locations, memList[i%len(memList)].Ip)
		}
		counter += FileCopiesNumber
	}
	*reply = WriteLocations{locations, recentUpdate}

	log.Println("Locations sended to requester")

	//Todo check Reply
	return nil
}

func (t *Socket) UpdateMap(args *FileStorageInfo, reply *string) error {
	fmt.Println("puts files")
	masterData[args.FileName] = args.Data
	duplicateMasterData() //send map to the backups
	fmt.Println(masterData)
	return nil
}

//store file into SDFS folder
func (t *Socket) StoreFile(args *File, reply *string) error {

	log.Println("Storing File......")
	//fmt.Println("File:", args)

	fileName := args.FileName
	fileData := args.Data
	putRequestTime := args.PutRequestTime

	myIp := GetOutboundIP()
	log.Println("In machine:" + myIp)
	log.Println("fileName:", fileName)
	log.Println("putRequestTime:", putRequestTime)

	path := "sdfs"
	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0700)
	}
	// f, err := os.OpenFile(path+"/"+fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	f, err := os.Create(path + "/" + fileName)
	if err != nil {
		log.Println(err)
	}
	_, err = f.Write(fileData)
	if err != nil {
		log.Println(err)
	}
	f.Close()

	// Todo: notifyWriteSucceed()
	localSDFSData[fileName] = putRequestTime
	logStoringFile(fileName)

	return nil
}

//only for the Introducer to create the list
func initList(myId id) []id {
	var MembershipList []id
	MembershipList = append(MembershipList, myId)
	return MembershipList
}

func startUDPServer(quitHeartbeaters chan bool, quitFailureListeners chan bool) {
	/* Lets prepare an address at any address at port 10001*/
	ServerAddr, err := net.ResolveUDPAddr("udp", ":10001")
	CheckError(err)

	/* Now listen at selected port */
	ServerConn, err := net.ListenUDP("udp", ServerAddr)
	CheckError(err)

	defer ServerConn.Close()
	chanMap := map[string]chan bool{}
	startHeartbeating(quitHeartbeaters, quitFailureListeners, &chanMap)

	for {
		// wait for UDP client to connect
		handleUDPConnection(ServerConn, quitHeartbeaters, quitFailureListeners, &chanMap)
	}
}

func getIndex(List *[]id, ip string) int {
	for i, item := range *List {
		if item.Ip == ip {
			return i
		}
	}
	return -1
}
func sendHeartBeatTo(ip string, quit <-chan bool) {
	//ServerAddr, err := net.ResolveUDPAddr("udp", ip+":10001")
	//CheckError(err)
	//LocalAddr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	//CheckError(err)
	//Conn, err := net.DialUDP("udp", LocalAddr, ServerAddr)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	Conn, err := net.Dial("udp", ip+":10001")
	CheckError(err)
	defer Conn.Close()
	for {
		select {
		case <-quit:
			return
		default:
			number := r.Intn(100)
			// fmt.Println(number)

			if number >= 0 {
				ip := GetOutboundIP()
				beat := createBeat(ip)
				b, _ := json.Marshal(beat)
				_, err := Conn.Write(b)
				if err != nil {
					fmt.Println(err)
				}
			}
			time.Sleep(time.Second / 4)
		}
	}
}
func getHeartbeatIds(List []id) []id {
	myIp := GetOutboundIP()
	i := getIndex(&List, myIp)
	numHeartbeats := min(HeartbeatCount, len(List)-1)
	var heartbeatIds []id
	for index := i + 1; index < i+1+numHeartbeats; index += 1 {
		heartbeatIds = append(heartbeatIds, List[index%len(List)])
	}
	return heartbeatIds
}

func getHeartbeatListenerIds(List []id) []id {
	myIp := GetOutboundIP()
	i := getIndex(&List, myIp)
	numHeartbeats := min(HeartbeatCount, len(List)-1)
	var heartbeatIds []id
	for index := len(List) + i - 1; index > len(List)+i-1-numHeartbeats; index -= 1 {
		heartbeatIds = append(heartbeatIds, List[index%len(List)])
	}
	return heartbeatIds
}

func getNumHeartbeaters() int {
	return min(HeartbeatCount, len(memList)-1)
}

func startHeartbeatListeners(quit <-chan bool, chanMap *map[string]chan bool) {
	heartbeatIds := getHeartbeatListenerIds(memList)
	for _, id := range heartbeatIds {
		channel := make(chan bool)
		(*chanMap)[id.Ip] = channel
		go listenForHeartbeatFrom(id, quit, channel)
	}
}

func listenForHeartbeatFrom(id id, quit <-chan bool, beatChan <-chan bool) {
	startTime := time.Now()
	ticker := time.NewTicker(timeOutTime * 1000 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			ticker.Stop()
			fmt.Println("time out")
			endTime := time.Now()
			fmt.Println(endTime.Sub(startTime))
			broadcast(utils.ServerIps, "fail", id)
		case <-quit:
			ticker.Stop()
			return
		case <-beatChan:
			ticker.Stop()
			ticker = time.NewTicker(timeOutTime * time.Second)
			//fmt.Println("alive")
			//fmt.Println(time.Now())
		}
	}
}
func startHeartbeatSenders(quit <-chan bool) {
	heartbeatIds := getHeartbeatIds(memList)
	for _, id := range heartbeatIds {
		go sendHeartBeatTo(id.Ip, quit)
	}
}

func stopChannels(numChannels int, quit chan<- bool) {
	for i := 0; i < numChannels; i++ {
		quit <- true
	}
}
func printMembershipList(List []id) {
	fmt.Println("The Membership list is:")
	for index, id := range List {
		fmt.Println(fmt.Sprintf("%d. %s", index+1, id))
	}
	fmt.Println()

}

//ask for membership list from ip addresses
func getMembershipList(ips []string) []id {
	var list []id
	for _, ip := range ips {
		fmt.Println(ip)
		hostName := ip
		portNum := "10001"
		service := hostName + ":" + portNum
		RemoteAddr, _ := net.ResolveUDPAddr("udp", service)

		conn, _ := net.DialUDP("udp", nil, RemoteAddr)

		defer conn.Close()

		m := message{
			Action: "introducerJoin",
			Data:   nil,
			The_id: id{},
		}
		b, _ := json.Marshal(m)

		//send it to the connection
		conn.Write(b)
		quit := make(chan bool)
		listReady := make(chan bool)
		go func() {
			time.Sleep(time.Second / 4)
			quit <- true
		}()
		go func() {
			buffer := make([]byte, 1024)
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			json.Unmarshal(buffer[0:n], &list)
			if err != nil {
				return
			}
			listReady <- true

		}()

		select {
		case <-quit:
			fmt.Println("Time out: No Reponse")
			break
		case <-listReady:
			fmt.Println("Membership List Found")
			return list
		}
	}
	return list

}

//handles incoming udp message
func handleUDPConnection(conn *net.UDPConn, quitHeartbeaters chan bool, quitFailureListeners chan bool, chanMap *map[string]chan bool) {

	buffer := make([]byte, 1024*1024)

	n, clientAddr, err := conn.ReadFromUDP(buffer)

	CheckError(err)
	//fmt.Println("UDP client : ", clientAddr)
	//fmt.Println("Received from UDP client :  ", string(buffer[:n]))

	m := decodeMessage(buffer[:n])

	switch m.Action {
	case "introducerJoin":
		fmt.Println("introducerJoin")
		b, _ := json.Marshal(memList)
		_, err = conn.WriteToUDP(b, clientAddr)
	case "join":
		fmt.Println("join")
		fmt.Println(m.The_id.Ip)
		stopHeartbeating(quitHeartbeaters, quitFailureListeners)
		memList = append(memList, m.The_id)
		sortList()
		// If handler is introducer, send List back to client
		if isIntroducer() {
			b, _ := json.Marshal(memList)
			_, err = conn.WriteToUDP(b, clientAddr)
		}
		startHeartbeating(quitHeartbeaters, quitFailureListeners, chanMap)
	case "leave":
		fmt.Println("leave")
		fmt.Println(m.The_id.Ip)
		stopHeartbeating(quitHeartbeaters, quitFailureListeners)
		deleteFromList(m.The_id.Ip)
		startHeartbeating(quitHeartbeaters, quitFailureListeners, chanMap)
	case "fail":
		//hearbeat system
		fmt.Println("fail")
		fmt.Println(m.The_id.Ip)
		stopHeartbeating(quitHeartbeaters, quitFailureListeners)
		deleted := deleteFromList(m.The_id.Ip)
		if deleted {
		}
		startHeartbeating(quitHeartbeaters, quitFailureListeners, chanMap)

		//file system
		// myIp := GetOutboundIP()

		failingFlag = true
		SuperStep = 0

		// go handleSDFSrepair(myIp)
		//go handleGraphProcessing()

	case "beat":
		if channel, ok := (*chanMap)[m.The_id.Ip]; ok {
			channel <- true
		}
	default:
		fmt.Println("Unexpected action:")
		fmt.Println(m.Action)
	}

}

//------------------------mp3---------------------------

func handleSDFSrepair(myIp string) {
	// log.Println("isCurrentMaster:", isCurrentMaster(myIp))
	if isCurrentMaster(myIp) {

		for {
			if !RepairInprogess { //wait for last repair (Assined To Me) to finish first
				RepairInprogess = true
				repairSDFS()
				break
			}
		}
	}
}
func repairSDFS() {

	log.Println(".......reparing SDPS........")
	FileToNewLocation := make(map[string][]string) //file : newlocation

	for fileName, filedata := range masterData { //iterates through files

		Increase := 1
		newLocations := []string{}

		for i, fileLocationIp := range filedata.Locations { //iterates through its locations
			if getIndexInList(fileLocationIp) == -1 { //not in current memList

				var newLocationIp string
				if i < 3 {
					newLocationIp = memList[(getIndexInList(filedata.Locations[3])+Increase)%len(memList)].Ip
				} else {
					newLocationIp = memList[(getIndexInList(filedata.Locations[2])+Increase)%len(memList)].Ip
				}

				FileToNewLocation[fileName] = append(FileToNewLocation[fileName], newLocationIp)

				Increase += 1
			} else {
				newLocations = append(newLocations, fileLocationIp)
			}
		}

		newFileData := FileData{newLocations, masterData[fileName].LastUpdate}
		masterData[fileName] = newFileData

	}

	if len(FileToNewLocation) == 0 {
		RepairInprogess = false
		return
	}

	var wg sync.WaitGroup

	for file, newlocations := range FileToNewLocation {
		wg.Add(len(newlocations))

		for _, newlocation := range newlocations {
			go func(file string, newlocation string) {
				defer wg.Done()

				//get latest file from live machines
				latestFileData, putRequestTime := getReplicatedFile(file, masterData[file].Locations)

				//Dial the server
				client, err := rpc.DialHTTP("tcp", newlocation+":12345")
				if err != nil {
					log.Println(err)
					return
				}
				// Synchronous call
				args := &File{file, latestFileData, putRequestTime}
				var reply string
				err = client.Call("Socket.StoreFile", args, &reply)
				if err != nil {
					log.Println(err)
				}
			}(file, newlocation)

		}

	}
	wg.Wait()
	log.Println(".......Finished Sending........")

	//Update MasterData
	for file, newlocations := range FileToNewLocation {
		oldLocations := masterData[file].Locations
		for _, newlocation := range newlocations {
			oldLocations = append(oldLocations, newlocation)
		}
		newFileData := FileData{oldLocations, masterData[file].LastUpdate}
		masterData[file] = newFileData
	}

	//duplicate MasterData
	duplicateMasterData()

	//set the flag back
	RepairInprogess = false
}
func listenCommandLine(myId id, quit chan<- bool) {
	fmt.Println("Enter command: ")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		cmd := scanner.Text()
		switch cmd {
		case "ml":
			printMembershipList(memList)
		case "id":
			fmt.Println("My id is:")
			fmt.Println(myId)
		case "leave":
			fmt.Println("Leaving the system")
			stopChannels(getNumHeartbeaters(), quit)
			broadcast(utils.ServerIps, "leave", myId)
			return
		case "store":
			printSDFSFilesOnMachine()
		default:
			words := strings.Fields(cmd)
			if len(words) == 0 {
				words = []string{"no input"}
			}
			op := words[0]
			switch op {
			case "put":
				if len(words) != 3 {
					fmt.Println("put useage: put <localfilename> <sdfsfilename>")
					break
				}
				putRequestTime = time.Now().String()
				localFilename := path.Base(words[1])
				remoteFilename := path.Base(words[2])
				putFileOnSDFS(localFilename, remoteFilename, putRequestTime)
			case "get":
				if len(words) != 2 {
					fmt.Println("get useage: get <sdfsfilename>")
					break
				}
				remoteFilename := path.Base(words[1])
				getFileOnSDFS(remoteFilename)
				logStoringFileInCurDir(remoteFilename)
			case "delete":
				if len(words) != 2 {
					fmt.Println("delete useage: delete <sdfsfilename>")
					break
				}
				remoteFilename := path.Base(words[1])
				deleteFileOnSDFS(remoteFilename)
			case "ls":
				if len(words) != 2 {
					fmt.Println("ls useage: ls <sdfsfilename>")
					break
				}
				remoteFilename := path.Base(words[1])
				listLocationsOnSDFS(remoteFilename)
			case "sava":
				if len(words) != 3 {
					fmt.Println("sava useage: sava <exefile> <inputgraphname>")
					break
				}
				putRequestTime = time.Now().String()
				masterIp := memList[0].Ip

				inputExeName := path.Base(words[1])
				remoteExeName := "exe.go"
				putFileOnSDFS(inputExeName, remoteExeName, putRequestTime)

				inputGraphName := path.Base(words[2])
				remoteGraphName := "inputgraph.txt"
				putFileOnSDFS(inputGraphName, remoteGraphName, putRequestTime)

				askToGetFile(remoteExeName, masterIp)
				askToGetFile(remoteGraphName, masterIp)

				//testRunExeFile(masterIp)
				askToStartSava(masterIp, GetOutboundIP())
			default:
				fmt.Println("Command not know")
			}
		}
		fmt.Println("Enter command: ")
	}
}

func (t *Socket) DeleteSDFSFile(args *string, reply *string) error {
	_, ok := localSDFSData[*args]
	if !ok {
		return nil
	}
	delete(localSDFSData, *args)
	os.Remove("sdfs/" + *args)
	logDeletingFile(*args)
	return nil
}

func (t *Socket) GetSDFSFile(args *string, reply *File) error {
	writeTime, ok := localSDFSData[*args]
	if !ok {
		return nil
	}
	dat, err := ioutil.ReadFile("sdfs/" + *args)
	CheckError(err)
	*reply = File{*args, dat, writeTime}
	return nil
}

func (t *Socket) BackupMasterData(args *map[string]FileData, reply *string) error {
	masterData = *args
	myIp := GetOutboundIP()
	*reply = "backup succeed in machine:" + myIp
	return nil
}
func (t *Socket) DeleteFileLocations(args *string, reply *string) error {
	delete(masterData, *args)
	return nil
}
func (t *Socket) GetFileLocations(args *string, reply *[]string) error {
	*reply = masterData[*args].Locations
	return nil
}
func deleteReplicatedFile(remoteFilename string, locations []string) {

	for i := 0; i < len(locations); i++ {
		ip := locations[i]
		go func(i int, ip string) {
			client, err := rpc.DialHTTP("tcp", ip+":12345")
			if err != nil {
				log.Println(err)
			}
			var reply string
			err = client.Call("Socket.DeleteSDFSFile", &remoteFilename, &reply)
			if err != nil {
				log.Println(err)
			}
		}(i, ip)
	}

}
func duplicateMasterData() []string {

	if len(memList) < 3 {
		return nil
	}

	var replies []string

	var wg sync.WaitGroup
	wg.Add(2)

	for i := 1; i < 3; i++ {
		ip := memList[i].Ip
		go func(i int, ip string) {
			log.Println("Destination Ip:", ip)
			defer wg.Done()

			//Dial the server
			client, err := rpc.DialHTTP("tcp", ip+":12345")
			if err != nil {
				log.Println(err)
				return
			}
			// Synchronous call
			var reply string
			err = client.Call("Socket.BackupMasterData", &masterData, &reply)
			if err != nil {
				log.Println(err)
			}
			replies = append(replies, reply)

		}(i, ip)
	}
	//find the most recent updated one
	wg.Wait()

	return replies
}

//return the lastest version of file in locations
func getReplicatedFile(remoteFilename string, locations []string) ([]byte, string) {

	var replies []File

	var wg sync.WaitGroup
	wg.Add(len(locations))

	for i := 0; i < len(locations); i++ {
		ip := locations[i]
		go func(i int, ip string) {
			defer wg.Done()

			//Dial the server
			client, err := rpc.DialHTTP("tcp", ip+":12345")
			if err != nil {
				log.Println(err)
				return
			}
			// Synchronous call
			var reply File

			err = client.Call("Socket.GetSDFSFile", &remoteFilename, &reply)
			if err != nil {
				log.Println(err)
			} else {
				replies = append(replies, reply)
			}

		}(i, ip)
	}

	//find the most recent updated one
	wg.Wait()
	maxTime, _ := time.Parse(TimeFormat, replies[0].PutRequestTime)
	maxIndex := 0
	for i := 0; i < len(replies); i++ {
		time, _ := time.Parse(TimeFormat, replies[i].PutRequestTime)
		if time.After(maxTime) {
			maxTime = time
			maxIndex = i
		}
	}
	LatestFileData := replies[maxIndex].Data
	return LatestFileData, maxTime.String()
}
func putFileOnSDFS(localFilename string, remoteFilename, putRequestTime string) {

	if _, err := os.Stat(localFilename); os.IsNotExist(err) {
		fmt.Println("File does not exist in local")
		return
	}

	masterIp := memList[0].Ip
	writeLocations := askForLocation(masterIp, remoteFilename, putRequestTime)
	if writeLocations.ForceNeeded {
		scanner := bufio.NewScanner(os.Stdin)
		start := time.Now()
		for {
			fmt.Println("File updated in last minute. Are you sure you want to overwrite the file? (Y/N)")
			scanner.Scan()
			cmd := scanner.Text()
			t := time.Now()
			if cmd == "N" {
				fmt.Println("Overwrite cancelled")
				return
			}
			if t.Sub(start) > 30*1000*time.Millisecond {
				fmt.Println("Timeout on confirmation - write not completed")
				return
			}
			if cmd == "Y" {
				//break out so that write can occur
				break
			}

		}

	}
	//Write on SDFS
	fmt.Println("write " + localFilename + " to SDFS with remote name " + remoteFilename)
	sendFiles(localFilename, remoteFilename, putRequestTime, writeLocations.Locations)

	//Tell Master to update map
	args := &FileStorageInfo{remoteFilename, FileData{writeLocations.Locations, putRequestTime}}
	client, err := rpc.DialHTTP("tcp", masterIp+":12345")
	var reply string
	err = client.Call("Socket.UpdateMap", args, &reply)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("File added to SDSF")

}
func getFileOnSDFS(remoteFilename string) error {
	masterIp := memList[0].Ip
	client, err := rpc.DialHTTP("tcp", masterIp+":12345")
	var locations []string
	err = client.Call("Socket.GetFileLocations", &remoteFilename, &locations)
	if err != nil {
		return err
	}
	if len(locations) == 0 {
		//log.Println("File not stored on SDFS")
		return errors.New("File not stored on SDFS")
	}
	latestFileData, _ := getReplicatedFile(remoteFilename, locations)
	writeToCurrentDir(remoteFilename, latestFileData)
	return nil
}
func deleteFileOnSDFS(remoteFilename string) {
	masterIp := memList[0].Ip
	client, err := rpc.DialHTTP("tcp", masterIp+":12345")
	var locations []string
	err = client.Call("Socket.GetFileLocations", &remoteFilename, &locations)
	if err != nil {
		log.Println(err)
	}
	if len(locations) == 0 {
		fmt.Println("File not stored on SDFS")
		return
	}
	var reply string
	err = client.Call("Socket.DeleteFileLocations", &remoteFilename, &reply)
	if err != nil {
		log.Println(err)
	}
	deleteReplicatedFile(remoteFilename, locations)
}
func listLocationsOnSDFS(remoteFilename string) {
	fmt.Println("List locations of " + remoteFilename + " on SDFS")
	masterIp := memList[0].Ip
	client, err := rpc.DialHTTP("tcp", masterIp+":12345")
	var reply []string
	err = client.Call("Socket.GetFileLocations", &remoteFilename, &reply)
	if err != nil {
		log.Println(err)
	}
	if len(reply) > 0 {
		fmt.Println("Locations:")
		for i := 0; i < len(reply); i++ {
			fmt.Println(reply[i])
		}
	} else {
		fmt.Println("File not stored on SDFS")
	}

	return
}
func writeToCurrentDir(fileName string, fileData []byte) {
	//write to current directory
	f, err := os.Create(fileName)
	if err != nil {
		panic(err)
	}
	f.Write([]byte(fileData))
	f.Close()
}

func printSDFSFilesOnMachine() {
	files, err := ioutil.ReadDir("./sdfs")
	if os.IsNotExist(err) {
		fmt.Println("No SDFS files stored on this machine.")
		return
	} else if err != nil {
		log.Fatal(err)
	}
	if len(files) == 0 {
		fmt.Println("No SDFS files stored on this machine.")
		return
	}
	fmt.Println("SDFS Files stores on this machine:")
	fmt.Println("==================================")
	for _, f := range files {
		fmt.Println(f.Name())
	}
	fmt.Println("==================================")
	return
}

func askForLocation(masterIp string, filename string, putRequestTime string) *WriteLocations {
	//TODO get location from master

	//Dial the server
	client, err := rpc.DialHTTP("tcp", masterIp+":12345")
	if err != nil {
		panic(err)
	}
	// Synchronous call
	args := &PutRequest{FileName: filename, PutRequestTime: putRequestTime}
	var reply WriteLocations
	err = client.Call("Socket.SendLocation", args, &reply)
	if err != nil {
		panic(err)
	}

	return &reply
}

//send a grep request to the given server address with the command as the grep
func sendFiles(localFileLocation string, remoteFileName string, putRequestTime string, locationIps []string) ([]string, error) {

	dat, _ := ioutil.ReadFile(localFileLocation)

	var replies []string
	var myerr error

	var wg sync.WaitGroup
	wg.Add(4)

	for i := 0; i < len(locationIps); i++ {
		ip := locationIps[i]
		go func(i int, ip string) {
			defer wg.Done()

			//Dial the server
			client, err := rpc.DialHTTP("tcp", ip+":12345")
			if err != nil {
				myerr = err
			}
			// Synchronous call
			args := &File{remoteFileName, dat, putRequestTime}
			var reply string
			err = client.Call("Socket.StoreFile", args, &reply)
			if err != nil {
				log.Println(err)
			}

			fmt.Println("Store success:", reply)
			replies = append(replies, reply)

		}(i, ip)
	}

	wg.Wait()

	if myerr != nil {
		return []string{}, myerr
	} else {
		return replies, nil
	}
}

func startRPCServer() {
	Socket := new(Socket)
	rpc.Register(Socket)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":12345")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}

//-----------------------------mp4-----------------------------
func reduceMessages(workerOutputs []WorkerOutput, numOfWorkers int) []map[string][]string {

	var MessagesMaps []map[string][]string
	for i := 0; i < numOfWorkers; i++ {
		MessagesMaps = append(MessagesMaps, make(map[string][]string))
	}

	for _, workerOutput := range workerOutputs {
		for id_string, incomingMessages := range workerOutput.MessageMap {
			// log.Println("id_string", id_string)
			id, _ := strconv.Atoi(id_string)
			MessagesMaps[id%numOfWorkers][id_string] = append(MessagesMaps[id%numOfWorkers][id_string], incomingMessages...)
		}
	}
	return MessagesMaps
}

func parseInputGraph(filename string, workerNum int) []map[string]VertexState {

	var myMaps []map[string]VertexState
	for i := 0; i < workerNum; i++ {
		myMaps = append(myMaps, make(map[string]VertexState))
	}

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if scanner.Text()[0] == '#' {
			continue
		}
		// log.Println(scanner.Text() + " endline")

		words := strings.Fields(scanner.Text())
		id1, _ := strconv.Atoi(words[0])
		// id2, _ := strconv.Atoi(words[1])

		VertexState1, ok := myMaps[id1%workerNum][words[0]]
		// VertexState2, _ := myMaps[id2%workerNum][words[0]]

		if ok {
			VertexState1.OutVertices = append(VertexState1.OutVertices, words[1])
			tempState := myMaps[id1%workerNum][words[0]]
			tempState.OutVertices = VertexState1.OutVertices
			myMaps[id1%workerNum][words[0]] = tempState

			// VertexState2.OutVertices = append(VertexState2.OutVertices, words[0])
			// tempState = myMaps[id2%workerNum][words[1]]
			// tempState.OutVertices = VertexState2.OutVertices
			// myMaps[id2%workerNum][words[1]] = tempState
		} else {
			myMaps[id1%workerNum][words[0]] = VertexState{"0", []string{words[1]}, []string{}, true}
			// myMaps[id2%workerNum][words[1]] = VertexState{"0", []string{words[0]}, []string{}, true}
		}
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return myMaps
}

func (t *Socket) GetFileFromSDFS(args *string, reply *string) error {
	err := getFileOnSDFS(*args)
	if err != nil {
		return err
	}
	*reply = "Downloaded file:" + *args + " from SDFS"
	return nil
}

//Don't let #VM1 or 2 be the client, please!
func (t *Socket) StartSava(args *string, reply *[]VertexValuePair) error {

	clientIp := *args
	clientIndex := getIndexInList(clientIp)

	numOfWorker := len(memList) - 3

	//first tell them to get the exe and inputgraph to local directory
	var wg sync.WaitGroup
	if restart == false {
		wg.Add(numOfWorker)
		for i := 2; i < len(memList); i++ {
			if memList[i].Ip != clientIp {
				go func(i int) {
					defer wg.Done()
					askToGetFile("exe.go", memList[i].Ip)
					askToGetFile("inputgraph.txt", memList[i].Ip)
				}(i)
			}
		}
		wg.Wait()
	}

	myMaps := parseInputGraph("inputgraph.txt", numOfWorker)
	log.Println("lens of Mymaps:", len(myMaps))

	var IncomingMessages []map[string][]string
	//Then tell them to start SAVA with the superstep 0
	for i := 0; i < TotalSuperStep; i++ {
		log.Println("In superStep:", SuperStep)
		workerOutputs := []WorkerOutput{}
		wg.Add(numOfWorker)
		for j := 2; j < len(memList); j++ {
			if memList[j].Ip != clientIp {
				go func(i int, j int) {

					myMapIndex := -1
					if j < clientIndex {
						myMapIndex = j - 2
					} else {
						myMapIndex = j - 3
					}

					defer wg.Done()

					var workerOutput WorkerOutput
					if i == 0 {
						workerOutput = askToProcessGraph1stTime(SuperStep, memList[j].Ip, myMaps[myMapIndex])
					} else {
						workerOutput = askToProcessGraph(SuperStep, memList[j].Ip, IncomingMessages[myMapIndex])
					}
					if i == TotalSuperStep-1 {
						updateVertexState(myMaps, workerOutput, myMapIndex)
					}
					workerOutputs = append(workerOutputs, workerOutput)

				}(i, j)
			}
			if failingFlag == true {
				go askToStartSava(memList[0].Ip, clientIp)
				restart = true
				failingFlag = false
				return nil
			}
		}
		wg.Wait()

		IncomingMessages = reduceMessages(workerOutputs, numOfWorker)
		// log.Println("Reduced Messages:", IncomingMessages)
		//when all finished, increment superStep
		SuperStep += 1
	}
	log.Println("graph process done")

	result := outputTop25vertices(myMaps)
	log.Println("Top 25 vertices with their values:", result)
	*reply = result
	return nil
}

// type WorkerOutput struct {
// 	VertexMap  map[string]VertexState
// 	MessageMap map[string][]string
// }
func tellmyselfStartSava() {

}
func outputTop25vertices(myMaps []map[string]VertexState) []VertexValuePair {

	result := []VertexValuePair{}

	for _, vertexmap := range myMaps {
		for id_string, vertexState := range vertexmap {
			result = append(result, VertexValuePair{id_string, vertexState.Value})
		}
	}

	sort.Slice(result, func(i, j int) bool { return result[i].value > result[j].value })

	return result[0:25]
}

func updateVertexState(myMaps []map[string]VertexState, workerOutput WorkerOutput, index int) {
	myMaps[index] = workerOutput.VertexMap
}

func updateLocalVertices(workerOuput WorkerOutput) {
	for id, vertexState := range workerOuput.VertexMap {
		tempState := LocalVertices[id]
		tempState.Value = vertexState.Value
		LocalVertices[id] = tempState
	}
}
func updateLocalVertices1stTime(workerOuput WorkerOutput) {
	for id, value := range workerOuput.VertexMap {
		LocalVertices[id] = value
	}
}

func clearUnecessaryData(workerOuput WorkerOutput) WorkerOutput {
	var smaller WorkerOutput
	vMap := make(map[string]VertexState)
	vm := workerOuput.VertexMap
	for id, vs := range vm {
		var vertexState VertexState
		vertexState.Value = vs.Value
		vMap[id] = vertexState
	}
	smaller.VertexMap = vMap
	smaller.MessageMap = workerOuput.MessageMap
	return smaller

}

func (t *Socket) ProcessGraph1stTime(args InstructMessageFirstTime, reply *WorkerOutput) error {
	SuperStep = args.SuperStep
	LocalVertices = args.VertexMap
	workerOuput := RunWorker(LocalVertices)
	updateLocalVertices1stTime(workerOuput)
	*reply = clearUnecessaryData(workerOuput)
	return nil
}

func (t *Socket) ProcessGraph(args InstructMessage, reply *WorkerOutput) error {
	SuperStep = args.SuperStep
	for id, messages := range args.IncomingMessageMap {
		vs := LocalVertices[id]
		vs.IncomingMessages = messages
		LocalVertices[id] = vs
	}
	workerOuput := RunWorker(LocalVertices)
	updateLocalVertices(workerOuput)
	*reply = clearUnecessaryData(workerOuput)
	return nil
}

//masterIp is either the first one on ML or the second one, depends on whether the client is the first one
// func findMaster() string {
// 	masterIp := memList[0].Ip
// 	myIp := GetOutboundIP()
// 	if masterIp == myIp {
// 		masterIp = memList[1].Ip
// 	}
// 	return masterIp
// }

func sendInputGraph(localFileLocation string, remoteFileName string, ip string) {
	dat, _ := ioutil.ReadFile(localFileLocation)
	client, err := rpc.DialHTTP("tcp", ip+":12345")
	if err != nil {
		log.Println("1161:", err)
	}
	// Synchronous call
	args := &File{remoteFileName, dat, putRequestTime}
	var reply string
	err = client.Call("Socket.StoreFile", args, &reply)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("Store Exe succeed:", reply)
}
func sendExeFile(localFileLocation string, remoteFileName string, ip string) {

	dat, _ := ioutil.ReadFile(localFileLocation)
	client, err := rpc.DialHTTP("tcp", ip+":12345")
	if err != nil {
		log.Println("1161:", err)
	}
	// Synchronous call
	args := &File{remoteFileName, dat, putRequestTime}
	var reply string
	err = client.Call("Socket.StoreFile", args, &reply)
	if err != nil {
		log.Println(err)
	}
	fmt.Println("Store Exe succeed:", reply)
}

func askToGetFile(remoteFileName string, ip string) {
	// if failingFlag == true || restart == true{
	// 	return
	// }
	log.Println("Asking " + ip + " to get file:" + remoteFileName)
	client, err := rpc.DialHTTP("tcp", ip+":12345")
	// Synchronous call
	args := remoteFileName
	var reply string
	err = client.Call("Socket.GetFileFromSDFS", args, &reply)
	if err != nil {
		log.Println(err)
	} else {
		log.Println(reply)
	}
}

func askToStartSava(ip string, clientIp string) {
	client, err := rpc.DialHTTP("tcp", ip+":12345")
	// Synchronous call
	args := clientIp
	var reply []VertexValuePair
	err = client.Call("Socket.StartSava", args, &reply)
	if err != nil {
		log.Println(err)
	}
	log.Println("Top 25 vertices with their values:", reply)
}

func askToProcessGraph1stTime(superstep int, ip string, vertexMap map[string]VertexState) WorkerOutput {
	client, err := rpc.DialHTTP("tcp", ip+":12345")
	if err != nil {
		// failingFlag = true
		return WorkerOutput{}
	}
	// Synchronous call
	args := InstructMessageFirstTime{superstep, vertexMap}
	var reply WorkerOutput
	err = client.Call("Socket.ProcessGraph1stTime", args, &reply)
	if err != nil {
		log.Println(err)
	}
	//log.Println("reply from worker: ", reply)
	return reply
}
func askToProcessGraph(superstep int, ip string, incomingMessageMap map[string][]string) WorkerOutput {
	client, err := rpc.DialHTTP("tcp", ip+":12345")
	if err != nil {
		// failingFlag = true
		return WorkerOutput{}
	}
	// Synchronous call
	args := InstructMessage{superstep, incomingMessageMap}
	var reply WorkerOutput
	err = client.Call("Socket.ProcessGraph", args, &reply)
	if err != nil {
		log.Println(err)
	}
	// item, ok := reply.VertexMap["1"]
	// if ok {
	// 	log.Println("reply from worker: ", item)
	// }
	return reply
}

//------------------MP4 exe interface work ------------------------
func RunWorker(vertices map[string]VertexState) WorkerOutput {
	log.Println("In super Step:", SuperStep)

	var messageMap map[string][]string
	messageMap = make(map[string][]string)
	count := 0
	for id, vs := range vertices {
		if count == 0 {
			log.Println(LocalVertices[id].OutVertices)
		}
		computeOutput := compute(vs, LocalVertices[id])
		vertices[id] = computeOutput.Vs
		for _, vertexMessage := range computeOutput.Messages {
			messageMap[vertexMessage.DestVertex] = append(messageMap[vertexMessage.DestVertex], vertexMessage.Value)
		}
		count += 1
	}
	return WorkerOutput{vertices, messageMap}
}

func compute(vs VertexState, localVS VertexState) ComputeOutput {

	var sum float64 = 0
	for _, v := range vs.IncomingMessages {
		f, _ := strconv.ParseFloat(v, 64)
		sum += f
	}

	value := -1.0
	if SuperStep == 0 {
		value = 1.0 / 334863
	} else {
		value = 0.15/334863 + 0.85*sum
	}

	vs.Value = strconv.FormatFloat(value, 'f', -1, 64)
	valueTosend := value / float64(len(localVS.OutVertices))
	messageValue := strconv.FormatFloat(valueTosend, 'f', -1, 64)
	vs.IncomingMessages = nil
	messages := []VertexMessage{}
	for _, id := range localVS.OutVertices {
		messages = append(messages, VertexMessage{id, messageValue})
	}

	return ComputeOutput{vs, messages}
}

func RunExe(vs VertexState) ComputeOutput {
	var result ComputeOutput
	//var message []VertexMessage
	words := []string{"run", "exe.go"}
	cmd := exec.Command("go", words...)

	stdout, err := cmd.StdoutPipe()
	stdin, err := cmd.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		log.Fatal(err)
	}
	enc := json.NewEncoder(stdin)
	enc.Encode(vs)
	if err := json.NewDecoder(stdout).Decode(&result); err != nil {
		log.Println(err)
	}

	if err := cmd.Wait(); err != nil {
		log.Println(err)
	}

	return result
}

//----------------------------main----------------------------------
func main() {
	os.Remove("inputgraph.txt")
	os.Remove("exe.go")
	f, err := os.Create("../" + LogFileName)
	os.RemoveAll("sdfs")
	if err != nil {
		panic(err)
	}
	f.Write([]byte("Start of Log:\n"))
	f.Close()
	//create id for node
	//Timestamp for id
	timeStamp := time.Now().String()
	ip := GetOutboundIP()
	myId := id{ip, timeStamp}
	quitHeartbeaters := make(chan bool)
	quitFailureListeners := make(chan bool)
	//run with additional command i to run as introducer
	//go run server.go i
	masterData = make(map[string]FileData)
	localSDFSData = make(map[string]string)
	if len(os.Args) > 1 && os.Args[1] == "i" && GetOutboundIP() == utils.ServerIps[0] {
		//this is for introducer
		memList = getMembershipList(utils.ServerIps[1:])
		if len(memList) == 0 {
			memList = initList(myId)
		} else {
			memList = append([]id{myId}, memList...)
			broadcast(utils.ServerIps[1:], "join", myId)
		}
	} else {
		//This is for others
		memList = broadcast(utils.ServerIps, "join", myId)
	}
	fmt.Println("List:", memList)
	go startUDPServer(quitHeartbeaters, quitFailureListeners)
	go startRPCServer()
	listenCommandLine(myId, quitHeartbeaters)

	// var args map[string]VertexState
	// args = make(map[string]VertexState)
	// vs := VertexState{"0.95", []string{"1.2", "1.5"}, []string{"1m", "m2"}, true}
	// args["v1"] = vs
	// fmt.Println(RunWorker(args))

	// parseInputGraph("amazongraph.txt", 7)

}
