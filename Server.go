package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"reflect"
	"time"

	pb "GoServer/protobuf/proto"
	"google.golang.org/protobuf/proto"
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
}

// Actor 模型中的 Actor
type Actor struct {
	conn   net.Conn
	worker Worker
}

type WorkerGroup struct {
	workers []Worker
}

// Worker 负责业务
type Worker struct {
	cancel  context.CancelFunc
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
	a := Actor{conn: conn, worker: workGroup.workers[randomIndex]}
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
			handleMessage(message)
		}
	}
}

func main() {
	// 启动线程池
	startWorkGroup()

	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("Error creating listener: %v\n", err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server listening on port %s\n", port)

	ctx, cancel := context.WithCancel(context.Background())
	// 使用一个通道来处理关闭信号
	shutdown := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt)
		<-sig
		fmt.Printf("收到关服信号!!!")
		cancel()
		close(shutdown)
	}()

	// 启动一个 goroutine 专门处理连接事件
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				conn, err := listener.Accept()
				if err != nil {
					continue
				}
				// 启动一个 goroutine 处理IO
				go handleConnection(ctx, conn)
			}
		}
	}()
	fmt.Printf("Server Startup Success!!!")
	// 使用通道等待关闭信号
	<-shutdown
	// 等待所有 worker 完成
	for _, w := range workGroup.workers {
		close(w.message)
		<-w.done
	}
	fmt.Printf("Server Shutdown!!!")
}

func handleConnection(ctx context.Context, conn net.Conn) {
	// 启动一个 goroutine 处理心跳检测
	//go heartbeatCheck(conn)

	actor := newActor(conn)

	// 处理其他逻辑
	readData(ctx, actor)
}

func handleMessage(message MessageCtx) {
	protoId := message.protoId
	if protoId == 700 {
		//fmt.Printf("receive data: %s", message.data)
		response := pb.ServerPongResponse{}
		sendData(message.actor, 701, &response)
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
			log.Printf("Error reading magic: %v", err)
			return
		}

		// 读取数据长度
		var msgLen uint32
		if err := binary.Read(conn, binary.BigEndian, &msgLen); err != nil {
			log.Printf("Error reading msgLen: %v", err)
			return
		}

		// 读取协议ID
		var protocolID int32
		if err := binary.Read(conn, binary.BigEndian, &protocolID); err != nil {
			log.Printf("Error reading protocol ID: %v", err)
			return
		}

		// 读取crc
		var crc int32
		if err := binary.Read(conn, binary.BigEndian, &crc); err != nil {
			log.Printf("Error reading protocol ID: %v", err)
			return
		}

		// 限制消息长度 1k
		if msgLen > msgMaxLen {
			fmt.Printf("Received message exceeds maximum msgLen: %d\n", msgLen)
			return
		}

		// 根据协议ID找到对应的消息类型
		messageType, ok := protocolIDToMessageType[protocolID]
		if !ok {
			log.Printf("Unknown protocol ID: %d", protocolID)
			return
		}
		// 创建对应的空消息实例
		message := reflect.New(messageType.Elem()).Interface().(proto.Message)

		// 读取数据
		payload := make([]byte, msgLen)
		_, err := io.ReadFull(conn, payload)
		if err != nil {
			log.Printf("Error reading payload: %v", err)
			return
		}

		// 反序列化消息
		if err := proto.Unmarshal(payload, message); err != nil {
			log.Printf("Failed to unmarshal payload: %v", err)
			return
		}

		// 将消息传给actor的工作线程
		actor.worker.message <- MessageCtx{actor: actor, protoId: protocolID, data: message}
	}
}

func sendData(actor Actor, protoId int32, message proto.Message) error {
	conn := actor.conn
	// 将消息序列化为字节流
	data, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("error marshalling response: %v", err)
	}

	// 写入数据长度
	if err := binary.Write(conn, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}

	// 写入协议号
	_ = binary.Write(conn, binary.BigEndian, uint32(protoId))

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
	// 添加其他协议ID和消息类型的映射
}

/*func main() {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		fmt.Printf("Error creating listener: %v\n", err)
		return
	}
	defer listener.Close()

	fmt.Printf("Server listening on port %s\n", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Error accepting connection: %v\n", err)
			continue
		}

		go handleConnection(conn)
	}
}*/

/*func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		// 读取数据
		data, err := readData(conn)
		if err != nil {
			fmt.Printf("Error reading data: %v\n", err)
			return
		}

		// 处理数据
		response := processData(data)

		// 发送响应
		err = sendData(conn, response)
		if err != nil {
			fmt.Printf("Error sending data: %v\n", err)
			return
		}
	}
}

func readData(conn net.Conn) (*pb.PingRequest, error) {
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("error reading data: %v", err)
	}

	request := &pb.PingRequest{}
	err = proto.Unmarshal(buffer[:n], request)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling data: %v", err)
	}

	return request, nil
}

func sendData(conn net.Conn, response *pb.PangResponse) error {
	data, err := proto.Marshal(response)
	if err != nil {
		return fmt.Errorf("error marshalling response: %v", err)
	}

	_, err = conn.Write(data)
	if err != nil {
		return fmt.Errorf("error writing data: %v", err)
	}

	return nil
}

func processData(request *pb.PingRequest) *pb.PangResponse {
	// 在这里处理请求并生成响应
	message := fmt.Sprintf("Received message: %s", request.Name)
	return &pb.PangResponse{Message: message}
}*/