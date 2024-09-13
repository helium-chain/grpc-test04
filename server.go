package main

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"

	pb "example.com/learn-grpc-04/ecommerce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	// 方法一
	// creds, err1 := credentials.NewServerTLSFromFile(
	//		"/root/workspace/learn-grpc/key/test.pem",
	//		"/root/workspace/learn-grpc/key/test.key",
	//	)
	//
	//	if err1 != nil {
	//		fmt.Printf("证书错误：%v", err1)
	//		return
	//	}

	// 方法二
	cert, err := tls.LoadX509KeyPair(
		"/root/workspace/learn-grpc-02/key/test.pem",
		"/root/workspace/learn-grpc-02/key/test.key")
	if err != nil {
		fmt.Printf("私钥错误：%v", err)
		return
	}
	creds := credentials.NewServerTLSFromCert(&cert)

	listen, _ := net.Listen("tcp", ":9090")
	grpcServer := grpc.NewServer(grpc.Creds(creds))
	pb.RegisterOrderManagementServer(grpcServer, &service{})

	// 启动服务
	err = grpcServer.Serve(listen)
	if err != nil {
		fmt.Println(err)
		return
	}
}

var _ pb.OrderManagementServer = (*service)(nil)

var orders = make(map[string]pb.Order, 8)

const orderBatchSize = 8

func init() {
	// 测试数据
	orders["101"] = pb.Order{Id: "1", Items: []string{"1", "2", "3", "4", "5", "7"}, Destination: "101"}
	orders["102"] = pb.Order{Id: "2", Items: []string{"6", "5", "4", "3", "2", "0"}, Destination: "102"}
}

type service struct {
	pb.UnimplementedOrderManagementServer
}

func (s *service) ProcessOrders(stream pb.OrderManagement_ProcessOrdersServer) error {
	batchMarker := 1
	var combinedShipmentMap = make(map[string]pb.CombinedShipment)
	for {
		orderId, err := stream.Recv()
		log.Printf("Reading Proc order : %s", orderId)
		if err == io.EOF {
			log.Printf("EOF : %s", orderId)
			for _, shipment := range combinedShipmentMap {
				if err := stream.Send(&shipment); err != nil {
					return err
				}
			}
			return nil
		}
		if err != nil {
			log.Println(err)
			return err
		}

		destination := orders[orderId.GetValue()].Destination
		shipment, found := combinedShipmentMap[destination]

		if found {
			ord := orders[orderId.GetValue()]
			shipment.OrderList = append(shipment.OrderList, &ord)
			combinedShipmentMap[destination] = shipment
		} else {
			comShip := pb.CombinedShipment{Id: "cmb - " + (orders[orderId.GetValue()].Destination), Status: "Processed!"}
			ord := orders[orderId.GetValue()]
			comShip.OrderList = append(shipment.OrderList, &ord)
			combinedShipmentMap[destination] = comShip
			log.Print(len(comShip.OrderList), comShip.GetId())
		}

		if batchMarker == orderBatchSize {
			for _, comb := range combinedShipmentMap {
				log.Printf("Shipping : %v -> %v", comb.Id, len(comb.OrderList))
				if err := stream.Send(&comb); err != nil {
					return err
				}
			}
			batchMarker = 0
			combinedShipmentMap = make(map[string]pb.CombinedShipment)
		} else {
			batchMarker++
		}
	}
}
