# SimpleGoServer

## 用go语言写的简单的服务端
添加GOROOT环境变量

添加GOPATH到环境变量PATH中
默认GOPATH 	C:\Users\用户名\go
PATH中添加 %GOPATH%\bin

安装生成proto的依赖库
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

生成pb.go用以下指令
protoc --go_out=. --go-grpc_out=. *.proto

安装linux交叉编译工具链
go get github.com/mitchellh/gox
生成linux可执行文件
gox -osarch="linux/amd64" -output="output/{{.Dir}}_{{.OS}}_{{.Arch}}"
