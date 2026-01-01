package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net"
	"time"
)

type Config struct {
	ListenAddr  string
	MaxClients  int
	IdleTimeout time.Duration
}

const defaultListenAddr = ":5001"

type Message struct {
	cmd  Command
	peer *Peer
}

type Server struct {
	Config
	semaphore chan struct{}
	peers     map[*Peer]bool
	ln        net.Listener
	addPeerCh chan *Peer
	delPeerCh chan *Peer
	quitCh    chan struct{}
	msgCh     chan Message
	kv        *KV
	ps        *PubSub
}

func NewServer(cfg Config) *Server {
	if len(cfg.ListenAddr) == 0 {
		cfg.ListenAddr = defaultListenAddr
	}

	if cfg.MaxClients == 0 {
		cfg.MaxClients = 10000
	}

	if cfg.IdleTimeout == 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}

	return &Server{
		Config:    cfg,
		semaphore: make(chan struct{}, cfg.MaxClients),
		peers:     make(map[*Peer]bool),
		addPeerCh: make(chan *Peer),
		delPeerCh: make(chan *Peer),
		quitCh:    make(chan struct{}),
		msgCh:     make(chan Message),
		kv:        NewKV(NopCache{}),
		ps:        NewPubSub(),
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.ListenAddr)
	if err != nil {
		return err
	}
	s.ln = ln
	go s.loop()

	slog.Info("Server Running", "ListenAddr", s.ListenAddr)

	return s.acceptLoop()
}

func (s *Server) handleMsg(msg Message) error {
	switch v := msg.cmd.(type) {
	case SetCommand:
		fmt.Println("Got Set Command for", v.key, v.val)
		s.kv.Set(v.key, v.val)
		_, err := msg.peer.Send([]byte(fmt.Sprintf("+OK\r\n")))
		if err != nil {
			fmt.Errorf("Peer send Errorf", err)
		}
		return nil

	case GetCommand:
		fmt.Println("Got Get Command for", string(v.key))
		val, ok := s.kv.Get(v.key)
		fmt.Println(val)
		fmt.Println("OK, ", ok)
		if !ok {
			_, err := msg.peer.Send([]byte("$-1\r\n"))
			if err != nil {
				return fmt.Errorf("failed to send error reply: %w", err)
			}
			return fmt.Errorf("Not able to find")
		} else {
			_, err := msg.peer.Send([]byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)))
			if err != nil {
				return fmt.Errorf("peer sent error: ", err)
			}
		}

	case DeleteCommand:
		s.kv.Delete(v.key)
		_, err := msg.peer.Send([]byte(fmt.Sprintf("$%d\r\n%d\r\n", 1, 1)))
		if err != nil {
			return fmt.Errorf("peer sent error: ", err)
		}
	case HelloCommand:
		spec := map[string]string{
			"server": "redis",
			"role":   "master",
		}
		_, err := msg.peer.Send(respWriteMap(spec))
		fmt.Println("Returning the hello command")
		if err != nil {
			return fmt.Errorf("peer sent error: ", err)
		}
	case ClientCommand:
		fmt.Println("Got Client Command")
		_, err := msg.peer.Send([]byte("+OK\r\n"))
		if err != nil {
			return fmt.Errorf("peer send error: ", err)
		}
	case SubscribeCommand:
		channelName := v.channel
		s.ps.Subscribe(channelName, msg.peer)
		confirm := fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n",
			len(channelName), channelName)
		_, err := msg.peer.Send([]byte(confirm))
		if err != nil {
			return fmt.Errorf("subscribe confirm error: %w", err)
		}
		return nil

	case UnsubscribeCommand:
		channelName := v.channel
		s.ps.Unsubscribe(channelName, msg.peer)
		confirm := fmt.Sprintf("*3\r\n$9\r\nunsubscribe\r\n$%d\r\n%s\r\n:1\r\n",
			len(channelName), channelName)
		_, err := msg.peer.Send([]byte(confirm))
		if err != nil {
			return fmt.Errorf("unsubscribe confirm error: %w", err)
		}
		return nil
	case ExistsCommand:
		_, ok := s.kv.Get(v.key)
		fmt.Println("Got into exist command", ok)
		var exists int
		if ok {
			exists = 1
		} else {
			exists = 0
		}

		// RESP integer reply
		resp := fmt.Sprintf(":%d\r\n", exists)
		_, err := msg.peer.Send([]byte(resp))
		if err != nil {
			return fmt.Errorf("peer send error: %w", err)
		}
		return nil
	case PublishCommand:
		channelName := v.channel
		numOfClients := s.ps.Write(channelName, v.val)
		resp := fmt.Sprintf(":%d\r\n", numOfClients)
		_, err := msg.peer.Send([]byte(resp))
		if err != nil {
			return fmt.Errorf("peer sent error: ", err)
		}
	case EchoCommand:
		data := v.key // First argument bytes
		fmt.Println("Got echo command for key:", string(data))

		// RESP bulk string: $<length>\r\n<data>\r\n
		resp := fmt.Sprintf("$%d\r\n%s\r\n", len(data), data)
		_, err := msg.peer.Send([]byte(resp))
		if err != nil {
			return fmt.Errorf("peer send error: %w", err)
		}
		return nil
	case FlushCommand:
		s.kv.Flush()
		resp := "+OK\r\n"
		_, err := msg.peer.Send([]byte(resp))
		if err != nil {
			return fmt.Errorf("peer send error: %w", err)
		}
		return nil
	case PingCommand:
		_, err := msg.peer.Send([]byte("+PONG\r\n"))
		if err != nil {
			fmt.Errorf("Peer send error: ", err)
		}
		return nil

	case QuitCommand:
		s.delPeerCh <- msg.peer
		return nil

	case CommandCommand:
		_, err := msg.peer.Send([]byte("*0\r\n"))
		if err != nil {
			fmt.Errorf("Peer send error: ", err)
		}
		return nil

	}
	return nil
}

func (s *Server) loop() {
	for {
		select {
		case rawMsg := <-s.msgCh:
			s.handleMsg(rawMsg)
		case <-s.quitCh:
			fmt.Println("Quiting the server")
			return
		case peer := <-s.addPeerCh:
			slog.Info("new peer connect", "remoteAddr", peer.conn.RemoteAddr())
			s.peers[peer] = true
		case peer := <-s.delPeerCh:
			slog.Info("new peer disconnected", "remoteAddr", peer.conn.RemoteAddr())
			delete(s.peers, peer)
			s.ps.RemovePeer(peer)
			// returning here will make server to timeout on next request coz the loop will end
		}
	}
}

func (s *Server) acceptLoop() error {
	for {
		conn, err := s.ln.Accept()
		if err != nil {
			slog.Error("Accept error", "err", err)
			continue
		}
		go s.handleConn(conn)
	}
}

func (s *Server) handleConn(conn net.Conn) {
	select {
	case s.semaphore <- struct{}{}:
		fmt.Println("Catching semaphore")
	default:
		conn.Close()
		slog.Warn("Max connection limit reached")
		return
	}

	peer := NewPeer(conn, s.msgCh, s.delPeerCh, s.semaphore)
	slog.Info("Peer Connected", "remoteAddr", conn.RemoteAddr())
	s.addPeerCh <- peer
	go peer.readLoop()
}

func main() {
	listenAddr := flag.String("listenAddr", defaultListenAddr, "listen address for redisgo server")

	server := NewServer(Config{
		ListenAddr: *listenAddr,
	})
	log.Fatal(server.Start())
}
