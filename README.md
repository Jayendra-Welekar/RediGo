# Redigo 🚀
**Redis-compatible in-memory key-value store built from scratch in Go**

---

## 🌟 Features

- **Full RESP2 Protocol Support** – Connect with official `redis-cli` and Go Redis clients (`go-redis`)
- **Core Commands**: `SET/GET/DEL/EXISTS/FLUSHDB/PING/ECHO/QUIT/HELLO`
- **Production-grade Pub/Sub** – `PUBLISH/SUBSCRIBE/UNSUBSCRIBE` with multi-channel support
- **Concurrent & Goroutine-safe** – Handles multiple simultaneous client connections
- **Custom Go Client Library** – Programmatic access + integration testing
- **Minimal CLI Tool** – Quick testing without external dependencies

## 🎯 Quick Start

Clone & build
git clone https://github.com/Jayendra-Welekar/RediGo.git
cd RediGo
go build -o redigo ./cmd/server

Run server (port 5001)
./redigo

Test with redis-cli (in another terminal)

redis-cli -p 5001

127.0.0.1:5001> PING

+PONG

127.0.0.1:5001> SET hello world

+OK

127.0.0.1:5001> GET hello

$5

world

127.0.0.1:5001> SUBSCRIBE news

"subscribe"

"news"

(integer) 1

text

## 🏗️ Architecture

Client (redis-cli/go-redis) ↔ TCP (net.Listener) ↔ RESP2 Parser ↔ Command Dispatcher
↓
Concurrent KV Store (sync.RWMutex) + Pub/Sub Manager (channels)

text

## 📋 Supported Commands

| Command | Status |
|---------|--------|
| `PING` | ✅ |
| `ECHO` | ✅ |
| `SET` | ✅ |
| `GET` | ✅ |
| `DEL` | ✅ |
| `EXISTS` | ✅ |
| `FLUSHDB` | ✅ |
| `PUBLISH` | ✅ |
| `SUBSCRIBE` | ✅ |
| `UNSUBSCRIBE` | ✅ |
| `QUIT` | ✅ |
| `HELLO` | ✅ |

## 🔧 Tech Stack

- **Go 1.23** – Systems programming
- **net package** – TCP networking
- **sync primitives** – Concurrency
- **RESP2 Protocol** – Redis wire protocol
- **tidwall/resp** – Protocol parsing

## 🚀 Why Redigo?

- **Deep Redis understanding** – Protocol, concurrency, networking
- **Production-ready patterns** – Goroutines, mutexes, channels
- **Real client compatibility** – No mock clients needed
- **Learning project** – Clean, well-tested codebase

## 🧪 Testing

Server integration tests
go test ./...

Test with go-redis client
go test ./client

text

## 📖 Learning Resources

- [Redis RESP Protocol](https://redis.io/docs/reference/protocol-spec/)
- [Go Concurrency Patterns](https://go.dev/blog/pipelines)
- [Building Redis from Scratch](https://codecrafters.io/challenges/build-your-own-redis)

---

**⭐ Star if you find it useful! Contributions welcome.**

---
*Built with ❤️ using Go. Compatible with Redis 7+ clients.*
