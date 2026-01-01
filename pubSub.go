package main

import (
	"fmt"
	"net"
	"slices"
	"sync"
)

type ChannelRoom struct {
	peers []*Peer
}

type PubSub struct {
	mu       sync.Mutex
	channels map[string]*ChannelRoom
	peerChan map[net.Conn]chan string
}

func (ps *PubSub) Subscribe(channel string, p *Peer) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    if _, exists := ps.channels[channel]; !exists {
        ps.channels[channel] = &ChannelRoom{peers: []*Peer{}}
    }
    
    if !slices.Contains(ps.channels[channel].peers, p) {
        ps.channels[channel].peers = append(ps.channels[channel].peers, p)
    } 
}

func (ps *PubSub) Unsubscribe(channel string, p *Peer) {
    ps.mu.Lock()
    defer ps.mu.Unlock()

    room, ok := ps.channels[channel]
    if !ok {
        return
    }

    if slices.Contains(room.peers, p) {
        room.peers = slices.DeleteFunc(room.peers, func(peer *Peer) bool {
            return peer == p
        })
    }

    if len(room.peers) == 0 {
        delete(ps.channels, channel) // optional: clean up empty channel
    }
}


func (ps *PubSub) Write(channel string, msg string) int {
	room, exists := ps.channels[channel]
	var numOfClients int = 0
	if exists {
		numOfClients = len(room.peers)
		for _, peer := range room.peers {
			sendPubSubMessage(peer.conn, channel, msg)
		}
	}

	return numOfClients
}

func sendPubSubMessage(conn net.Conn, channel, message string) error {
	// Prepare RESP message format
	resp := fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(channel), channel, len(message), message)

	// Write to the client's connection
	_, err := conn.Write([]byte(resp))
	return err
}

func NewPubSub() *PubSub {
	return &PubSub{
		channels: make(map[string]*ChannelRoom),
		peerChan: make(map[net.Conn]chan string),
	}
}

func (ps *PubSub) RemovePeer(p *Peer) {
    ps.mu.Lock()
    defer ps.mu.Unlock()
    for ch, room := range ps.channels {
        // Remove this peer from the channel's peers slice
        room.peers = slices.DeleteFunc(room.peers, func(peer *Peer) bool {
            return peer == p
        })
        if len(room.peers) == 0 {
            delete(ps.channels, ch) // optional: clean up empty channels
        }
    }
}

