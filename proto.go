package main

import (
	"bytes"
	"fmt"
)

const (
	CommandSET    = "set"
	CommandQuit   = "QUIT"
	CommandGET    = "get"
	CommandCLIENT = "client"
	CommandHELLO  = "hello"
	CommandDELETE = "del"
	CommandSUBSCRIBE = "subscribe"
	CommandUNSUBSCRIBE = "unsubscribe"
	CommandPUBLISH = "publish"
	CommandEXISTS = "exists"
	CommandECHO = "echo"
	CommandPING = "ping"
	CommandFLUSH = "flushdb"
	CommandCOMMAND = "COMMAND"
)

type Command interface{}

type CommandCommand struct {
}

type PingCommand struct {
}

type EchoCommand struct {
	key []byte
}

type SetCommand struct {
	key, val []byte
}

type QuitCommand struct{}

type GetCommand struct {
	key []byte
}

type HelloCommand struct {
	value string
}

type ClientCommand struct {
	client string
}

type DeleteCommand struct {
	key []byte
} 

type ExistsCommand struct {
	key []byte
}

type SubscribeCommand struct {
	channel string
}

type UnsubscribeCommand struct {
	channel string
}

type PublishCommand struct {
	channel string
	val string
}

type FlushCommand struct {
}

func respWriteMap(m map[string]string) []byte {
	buf := &bytes.Buffer{}
	buf.WriteString("%" + fmt.Sprintf("%d\r\n", len(m)))

	for k, v := range m {
		// Key as bulk string
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(k), k))
		// Value as bulk string
		buf.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(v), v))
	}
	return buf.Bytes()
}
