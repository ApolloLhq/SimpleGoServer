package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"reflect"
	"time"

	pb "GoServer/protobuf/proto"
	"github.com/xtaci/kcp-go"

	_ "net/http/pprof"
)

const (
	port           = ":7080"
	msgMaxLen      = 1024
	workerPoolSize = 32
)

type MessageCtx struct {
	actor   Actor
	protoId int32
	data    proto.Message
	conv    int32
}

// Actor 模型中的 Actor
type Actor struct {
	conn          net.Conn
	worker        Worker
	heartbeatTime int64
}

type WorkerGroup struct {
	workers []Worker
}

// Worker 负责业务
type Worker struct {
	message chan MessageCtx // 任务队列
	done    chan struct{}
}

var workGroup WorkerGroup

func startWorkGroup() {
	// 启动工作线程池
	for i := 0; i < workerPoolSize; i++ {
		w := Worker{message: make(chan MessageCtx, 10000), done: make(chan struct{})}
		workGroup.workers = append(workGroup.workers, w)
		go w.run()
	}
}

func newActor(conn net.Conn) Actor {
	// 设置随机数种子
	rand.New(rand.NewSource(time.Now().UnixNano()))
	// 生成一个随机的索引值
	randomIndex := rand.Intn(workerPoolSize)
	a := Actor{conn: conn, worker: workGroup.workers[randomIndex], heartbeatTime: time.Now().Unix()}
	return a
}

func (worker *Worker) run() {
	defer close(worker.done)

	for {
		select {
		case message, ok := <-worker.message:
			if !ok {
				// 通道已关闭，工作线程退出
				return
			}
			//conn := message.actor.conn
			func() {
				defer func() {
					if r := recover(); r != nil {
						fmt.Println("Recovered:", r)
					}
				}()
				handleMessage(message)
			}()
		}
	}
}

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// 启动线程池
	startWorkGroup()

	tcpListener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("Error creating tcpListener: %v\n", err)
		return
	}
	defer tcpListener.Close()

	//listenAddress := "0.0.0.0" + port
	kcpListener, err := kcp.ListenWithOptions("0.0.0.0:7080", nil, 0, 0)
	if err != nil {
		fmt.Printf("Error creating udpListener: %v\n", err)
		return
	}
	defer kcpListener.Close()

	fmt.Printf("TCPServer listening on port %s\n", port)
	fmt.Printf("KCPServer listening on port %s\n", port)

	ctx, cancel := context.WithCancel(context.Background())
	// 使用一个通道来处理关闭信号
	shutdown := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		fmt.Println("收到关服信号!!!")
		cancel()
		close(shutdown)
	}()

	// 启动一个 goroutine 专门处理tcp连接事件
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, err := tcpListener.Accept()
				if err != nil {
					continue
				}
				// 启动一个 goroutine 处理IO
				go handleConnection(ctx, conn)
			}
		}
	}()
	fmt.Println("TCPServer Startup Success!!!")

	// 启动一个 goroutine 专门处理udp连接事件
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, err := kcpListener.AcceptKCP()
				if err != nil {
					fmt.Printf("%s\n", err)
					continue
				}
				//log.Println("Server receive kcp connect:" + conn.RemoteAddr().String())
				go handleConnection(ctx, conn)
			}
		}
	}()
	fmt.Println("UDPServer Startup Success!!!")

	//go delayTask()

	// 使用通道等待关闭信号
	<-shutdown
	// 等待所有 worker 完成
	for _, w := range workGroup.workers {
		close(w.message)
		<-w.done
	}
	fmt.Println("Server Shutdown!!!")
}

func handleConnection(ctx context.Context, conn net.Conn) {
	actor := newActor(conn)
	// 启动一个 goroutine 处理心跳检测
	go heartbeatCheck(ctx, actor)

	// 处理其他逻辑
	readData(ctx, actor)
}

func handleMessage(message MessageCtx) {
	protoId := message.protoId
	//fmt.Printf("receive data: %s\n", message.data)
	if protoId == 700 {
		pingReq := message.data.(*pb.ClientPingRequest)
		response := pb.ServerPongResponse{Bytes: pingReq.Bytes}
		err := sendData(message.actor, 701, &response)
		if err != nil {
			return
		}
	}

	if protoId == 702 {
		response := pb.KcpConnectRsp{}
		// 使用类型断言将 net.Conn 转换为 kcp.UDPSession
		kcpConn, ok := message.actor.conn.(*kcp.UDPSession)
		if !ok {
			log.Fatal("Failed to convert net.Conn to kcp.UDPSession")
		}
		response.ConvId = kcpConn.GetConv()
		err := sendData(message.actor, 703, &response)
		if err != nil {
			return
		}
	}
}

func readData(ctx context.Context, actor Actor) {
	defer actor.conn.Close()
	for {
		select {
		case <-ctx.Done():
			// context 被取消，结束读取
			return
		default:
			// 继续读取数据
		}
		conn := actor.conn

		// 读取魔数
		var magic int32
		if err := binary.Read(conn, binary.BigEndian, &magic); err != nil {
			if err == io.EOF {
				//log.Println("Connection closed by client")
				return
			}
			//log.Printf("Error reading magic: %v", err)
			return
		}

		// 读取数据长度
		var msgLen uint32
		if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
			//log.Printf("Error reading msgLen: %v", err)
			return
		}

		// 读取协议ID
		var protocolID int32
		if err := binary.Read(conn, binary.BigEndian, &protocolID); err != nil {
			//log.Printf("Error reading protocol ID: %v", err)
			return
		}
		if protocolID <= 0 {
			//log.Printf("invalid protocol ID: %v", protocolID)
			return
		}

		// 读取crc
		var crc int32
		if err := binary.Read(conn, binary.BigEndian, &crc); err != nil {
			//log.Printf("Error reading crc: %v", err)
			return
		}

		// 限制消息长度 1k
		if msgLen < 0 || msgLen > msgMaxLen {
			fmt.Printf("Received message invalid msgLen: %d\n", msgLen)
			return
		}

		// 根据协议ID找到对应的消息类型
		messageType, ok := protocolIDToMessageType[protocolID]
		if !ok {
			//log.Printf("Unknown protocol ID: %d", protocolID)
			return
		}
		// 创建对应的空消息实例
		message := reflect.New(messageType.Elem()).Interface().(proto.Message)

		// 读取数据
		payload := make([]byte, msgLen)
		_, err := io.ReadFull(conn, payload)
		if err != nil {
			//log.Printf("Error reading payload: %v", err)
			return
		}

		// 反序列化消息
		if err := proto.Unmarshal(payload, message); err != nil {
			log.Printf("Failed to unmarshal payload: %v", err)
			return
		}
		// 将消息传给actor的工作线程
		actor.worker.message <- MessageCtx{actor: actor, protoId: protocolID, data: message}
		actor.heartbeatTime = time.Now().Unix()
	}
}

func sendData(actor Actor, protoId int32, message proto.Message) error {
	conn := actor.conn
	// 将消息序列化为字节流
	data, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("error marshalling response: %v", err)
	}

	/*// 写入数据长度
	if err := binary.Write(conn, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}

	// 写入协议号
	_ = binary.Write(conn, binary.BigEndian, uint32(protoId))

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}*/

	sendBytes := make([]byte, 4+4+len(data))
	lenBytes := make([]byte, 4)
	protoIdBytes := make([]byte, 4)

	binary.BigEndian.PutUint32(lenBytes, uint32(len(data)))
	binary.BigEndian.PutUint32(protoIdBytes, uint32(protoId))
	copy(sendBytes[:4], lenBytes)
	copy(sendBytes[4:8], protoIdBytes)
	copy(sendBytes[8:], data)

	_, err = conn.Write(sendBytes)
	if err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}
	return nil
}

func sendReq(conn net.Conn, protoId int32, message proto.Message) error {
	// 将消息序列化为字节流
	data, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("error marshalling response: %v", err)
	}

	// 写入魔数
	if err := binary.Write(conn, binary.BigEndian, uint32(520)); err != nil {
		return err
	}

	// 写入数据长度
	if err := binary.Write(conn, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}

	// 写入协议号
	if err := binary.Write(conn, binary.BigEndian, uint32(protoId)); err != nil {
		return err
	}

	// 写入crc
	if err := binary.Write(conn, binary.BigEndian, uint32(0)); err != nil {
		return err
	}

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}
	return nil
}

// 在这里添加协议ID到消息类型的映射
var protocolIDToMessageType = map[int32]reflect.Type{
	// int CLIENT_PING = 700;
	// int SERVER_PONG = 701;
	700: reflect.TypeOf(&pb.ClientPingRequest{}),
	701: reflect.TypeOf(&pb.ServerPongResponse{}),

	// int KCP_REQ = 702
	702: reflect.TypeOf(&pb.KcpConnectReq{}),
	703: reflect.TypeOf(&pb.KcpConnectRsp{}),
	// int KCP_RSP = 703
	// 添加其他协议ID和消息类型的映射
}

func heartbeatCheck(ctx context.Context, actor Actor) {
	for {
		// 创建一个定时器，设置定时时间为80秒
		timer := time.NewTimer(80 * time.Second)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			conn := actor.conn
			heartbeatTime := actor.heartbeatTime
			// 检查是否超过80秒没有合法数据
			if time.Now().Unix()-heartbeatTime > 80 {
				//log.Println("No heartbeat received. Closing connection:", conn.RemoteAddr().String())
				conn.Close()
				return
			}
		}
	}
}

func delayTask() {
	for {
		go func() {
			conn, err := kcp.DialWithOptions("127.0.0.1:7080", nil, 0, 0)
			if err != nil {
				log.Fatal(err)
			}
			defer conn.Close()

			//fmt.Println("Connected to KCP server")
			request := pb.KcpConnectReq{}
			sendReq(conn, 702, &request)
			var rspLen, rspProtoId int32
			binary.Read(conn, binary.BigEndian, &rspLen)
			binary.Read(conn, binary.BigEndian, &rspProtoId)

			// 读取数据
			payload := make([]byte, rspLen)
			io.ReadFull(conn, payload)

			// 根据协议ID找到对应的消息类型
			messageType := protocolIDToMessageType[rspProtoId]
			// 创建对应的空消息实例
			message := reflect.New(messageType.Elem()).Interface().(proto.Message)
			// 反序列化消息
			if err := proto.Unmarshal(payload, message); err != nil {
				log.Printf("Failed to unmarshal payload: %v", err)
				return
			}

			log.Printf("Client receive Rsp len: %d, protoId: %d", rspLen, rspProtoId)
			log.Printf("client receive data: %s", message)
			time.Sleep(5 * time.Second)
		}()
		time.Sleep(5 * time.Second)
	}
}
