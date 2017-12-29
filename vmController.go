package main

import (
	"./creds"
	"./utils"
	"bufio"
	"fmt"
	"golang.org/x/crypto/ssh"
	"io"
	"log"
	"net"
	"os"
	// "strconv"
	"strings"
	"sync"
)

type Connection struct {
	*ssh.Client
	password string
}

func main() {

	argsWithoutProg := os.Args[1:]

	var cmd string
	if len(argsWithoutProg) > 0 {
		cmd = argsWithoutProg[0]
	}

	//parallel execution
	var wg sync.WaitGroup
	wg.Add(len(utils.Server_addresses))

	for i, server := range utils.Server_addresses {
		go func(i int, server string) {

			defer wg.Done()

			// log.Println("In server:", server)
			conn, err := Connect(server, cred.Username, cred.Password)
			if err != nil {
				log.Fatal(err)
			}

			var output []byte

			switch cmd {
			case "start":
				output, err = conn.ExecCommands("cd ../shared/cs425-mp4", "go run server.go &")
			//case "stop":
			//output, err = conn.ExecCommands("/usr/sbin/fuser -k 1234/tcp")
			case "clear":
				output, err = conn.ExecCommands("cd ../shared", "echo > machine.log")
			case "pull":
				output, err = conn.ExecCommands("cd ../shared/cs425-mp4/", "sudo git pull")
			case "reclone":
				output, err = conn.ExecCommands("cd ../shared/", "sudo rm -rf cs425-mp4", "git clone https://gitlab.engr.illinois.edu/CS425_cebenso2_tchen72/cs425-mp4.git")
			case "installscala":
				output, err = conn.ExecCommands("cd ../shared/", "wget https://github.com/sbt/sbt/releases/download/v1.0.4/sbt-1.0.4.zip")
			default:
				output, err = conn.ExecCommands("cd ../shared/cs425-mp4/", "sudo tar -zxf spark-2.2.0-bin-hadoop2.7.tgz")
				//output, err = conn.ExecCommands("cd ../shared", "git clone https://gitlab.engr.illinois.edu/CS425_cebenso2_tchen72/cs425-mp4.git")
				// output, err = conn.ExecCommands("sudo yum install iftop")
			}

			if err != nil {
				log.Println(err)
			}

			fmt.Println("----------In VM ", i+1, "----------------"+string(output))
		}(i, server)
	}

	// wait for goroutines to finish
	wg.Wait()

}

func Connect(addr, user, password string) (*Connection, error) {
	sshConfig := &ssh.ClientConfig{
		User: user,
		Auth: []ssh.AuthMethod{
			ssh.Password(password),
		},
		HostKeyCallback: ssh.HostKeyCallback(func(hostname string, remote net.Addr, key ssh.PublicKey) error { return nil }),
	}

	conn, err := ssh.Dial("tcp", addr+":22", sshConfig)
	if err != nil {
		return nil, err
	}

	return &Connection{conn, password}, nil

}

func (conn *Connection) ExecCommands(cmds ...string) ([]byte, error) {

	session, err := conn.NewSession()
	if err != nil {
		log.Fatal(err)
	}
	defer session.Close()

	if cmds[len(cmds)-1] != "nohup go run start_servers.go > /dev/null 2>&1 &" {
		modes := ssh.TerminalModes{
			ssh.ECHO:          0,     // disable echoing
			ssh.TTY_OP_ISPEED: 14400, // input speed = 14.4kbaud
			ssh.TTY_OP_OSPEED: 14400, // output speed = 14.4kbaud
		}

		err = session.RequestPty("xterm", 80, 40, modes)
		if err != nil {
			return []byte{}, err
		}

	}

	in, err := session.StdinPipe()
	if err != nil {
		log.Fatal(err)
	}

	out, err := session.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	var output []byte

	go func(in io.WriteCloser, out io.Reader) {
		var (
			line string
			r    = bufio.NewReader(out)
		)
		for {
			b, err := r.ReadByte()
			if err != nil {
				break
			}

			output = append(output, b)

			if b == byte('\n') {
				line = ""
				continue
			}

			// log.Println(line)
			line += string(b)

			if strings.HasPrefix(line, "Username for ") && strings.HasSuffix(line, ": ") {
				_, err = in.Write([]byte(cred.Username + "\n"))
				line = ""
				if err != nil {
					break
				}
			} else if strings.HasPrefix(line, "Password for ") && strings.HasSuffix(line, ": ") {
				_, err = in.Write([]byte(cred.Password + "\n"))
				line = ""
				if err != nil {
					break
				}
			} else if strings.HasPrefix(line, "[sudo] password for ") && strings.HasSuffix(line, ": ") {
				_, err = in.Write([]byte(cred.Password + "\n"))
				line = ""
				if err != nil {
					break
				}
			} else if strings.HasPrefix(line, "Is this ok") && strings.HasSuffix(line, ": ") {
				_, err = in.Write([]byte("y" + "\n"))
				line = ""
				if err != nil {
					break
				}
			}
		}
	}(in, out)

	cmd := strings.Join(cmds, "; ")
	// log.Println("received cmd:", cmd)
	err = session.Run(cmd)
	// if err != nil {
	// 	return []byte{}, err
	// }

	return output, nil
}
