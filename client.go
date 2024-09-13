package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "example.com/learn-grpc-04/ecommerce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func main() {
	creds, _ := credentials.NewClientTLSFromFile(
		"/root/workspace/learn-grpc-02/key/test.pem",
		"*.heliu.site",
	)

	var opts []grpc.DialOption
	// 不带TLS这里是grpc.WithTransportCredentials(insecure.NewCredentials())
	opts = append(opts, grpc.WithTransportCredentials(creds))

	// 连接server端，使用ssl加密通信
	conn, err := grpc.NewClient("127.0.0.1:9090", opts...)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}

	defer conn.Close()

	client := pb.NewOrderManagementClient(conn)

	fmt.Printf("now-Time: %s\n", time.Now().Format(time.DateTime))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.ProcessOrders(ctx)

	if err != nil {
		log.Fatalf("error when calling SearchOrders: %v", err)
	}

	go func() {
		if err := stream.Send(&wrapperspb.StringValue{Value: "101"}); err != nil {
			panic(err)
		}

		if err := stream.Send(&wrapperspb.StringValue{Value: "102"}); err != nil {
			panic(err)
		}

		if err := stream.CloseSend(); err != nil {
			panic(err)
		}
	}()

	for {
		combinedShipment, err := stream.Recv()
		if err == io.EOF {
			break
		}
		log.Println("Combined shipment : ", combinedShipment.OrderList)
	}
}
