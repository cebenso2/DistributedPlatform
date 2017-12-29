Instructions: 

1. First you need to install go and set up correct GoPath in your computer.
2. If you would like to use our VM Controller so that you could start of kill all 10 VM by one command: 
   1. Install the package golang.org/x/crypto/ssh by running the command: go get -u golang.org/x/crypto/...
   2. Create a folder named: creds inside the cs425-mp1 folder. 
   3. Then create a file inside that folder called cred.go
   4. Add three lines of code to it:
      1. package cred
      2. Const Username = “yourUsername”
      3. Const Password = “yourPassword”

5. To start and join the server:
      1. cd shared/
      2. go run server.go
6. To start the introducer, go to VM1:
      1. cd shared/
      2. go run server.go i
7. once running commands:
   1. ml - print memberlist
   2. id - prints id
   3. leave - leaves program
   4. put localfilename remote
   5. get remote
   6. delete remote
   7. ls remote
   8. store
   9. sava <exe.go> <inputfile.go>

