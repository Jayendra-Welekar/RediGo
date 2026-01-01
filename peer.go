package main

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/tidwall/resp"
)

type Peer struct {
	conn        net.Conn
	semaphore   chan struct{}
	semRelease  func()
	msgCh       chan Message
	delCh       chan *Peer
	lastActive  time.Time
	idleTimeout time.Duration
}

func (p *Peer) Send(msg []byte) (int, error) {
	return p.conn.Write(msg)
}

func NewPeer(conn net.Conn, msgCh chan Message, delCh chan *Peer, semaphore chan struct{}) *Peer {
	return &Peer{
		conn:        conn,
		semaphore:   semaphore,
		semRelease:  func() { fmt.Println("Releasing Semaphore"); <-semaphore },
		msgCh:       msgCh,
		delCh:       delCh,
		lastActive:  time.Now(),
		idleTimeout: time.Minute * 30,
	}
}

func (p *Peer) readLoop() error {
	defer p.semRelease()

	idleCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(p.idleTimeout)
		defer ticker.Stop()

		for {
			<-ticker.C
			if time.Since(p.lastActive) > p.idleTimeout {
				close(idleCh)
				return
			}
		}
	}()

	rd := resp.NewReader(p.conn)

	for {
		select {
		case <-idleCh:
			p.delCh <- p
			return nil
		default:
			v, _, err := rd.ReadValue()

			if err == io.EOF {
				fmt.Println("We Are currently at EOF")
				p.delCh <- p
				return nil
			}

			if err != nil {
				// Log and cleanly delete peer on error
				fmt.Println("Read error:", err)
				p.delCh <- p
				return err
			}

			if v.Type() != resp.Array || len(v.Array()) == 0 {
				p.Send([]byte("-ERR protocol error\r\n"))
				continue
			}

			cmdName := v.Array()[0].String()
			var cmd Command
			switch cmdName {

			case CommandPING:
				cmd = PingCommand{}

			case CommandECHO:
				if len(v.Array()) < 2 {
					p.Send([]byte("-ERR wrong number of arguments for ECHO\r\n"))
					continue
				}
				cmd = EchoCommand{key: v.Array()[1].Bytes()}

			case CommandGET:
				if len(v.Array()) != 2 {
					p.Send([]byte("-ERR wrong number of arguments for GET\r\n"))
					continue
				}
				cmd = GetCommand{key: v.Array()[1].Bytes()}

			case CommandSET:
				if len(v.Array()) != 3 {
					p.Send([]byte("-ERR wrong number of arguments for SET\r\n"))
					continue
				}
				cmd = SetCommand{
					key: v.Array()[1].Bytes(),
					val: v.Array()[2].Bytes(),
				}
			case CommandHELLO:
				if len(v.Array()) < 2 {
					p.Send([]byte("-ERR wrong number of arguments for HELLO\r\n"))
					continue
				}
				cmd = HelloCommand{value: v.Array()[1].String()}

			case CommandDELETE:
				if len(v.Array()) < 2 {
					p.Send([]byte("-ERR wrong number of arguments for HELLO\r\n"))
					continue
				}
				cmd = DeleteCommand{key: v.Array()[1].Bytes()}
			case CommandCLIENT:
				p.Send([]byte("+OK\r\n"))
				continue

			case CommandSUBSCRIBE:
				if len(v.Array()) < 2 {
					p.Send([]byte("-ERR wrong number of arguments for HELLO\r\n"))
					continue
				}
				cmd = SubscribeCommand{
					channel: v.Array()[1].String(),
				}

			case CommandUNSUBSCRIBE:
				fmt.Println("Received Unsubscribe command")
				if len(v.Array()) < 2 {
					p.Send([]byte("-ERR wrong number of arguments for HELLO\r\n"))
					continue
				}
				cmd = UnsubscribeCommand{
					channel: v.Array()[1].String(),
				}

			case CommandEXISTS:
				fmt.Println("Got INTO EXISTS Command")
				if len(v.Array()) < 1 {
					p.Send([]byte("-ERR wrong number of arguments for HELLO\r\n"))
					continue
				}
				cmd = ExistsCommand{
					key: v.Array()[1].Bytes(),
				}

			case CommandPUBLISH:
				if len(v.Array()) < 2 {
					p.Send([]byte("-ERR wrong number of arguments for HELLO\r\n"))
					continue
				}
				cmd = PublishCommand{
					channel: v.Array()[1].String(),
					val:     v.Array()[2].String(),
				}

			case CommandFLUSH:
				cmd = FlushCommand{}

			case CommandCOMMAND:
				cmd = CommandCommand{}
			default:
				fmt.Println("This is the command received", cmdName)
				continue
			}

			p.msgCh <- Message{cmd: cmd, peer: p}
			p.lastActive = time.Now()
		}
	}
	return nil
}
