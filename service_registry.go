package service_registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
)

var (
	serviceRegistry    *ServiceRegistry
	once               sync.Once
	serviceKeyTemplate = "/dataflows/%s"
)

type ServiceRegistry struct {
	client    *clientv3.Client
	serviceId int
}

func Register(host string, serviceId int) {
	once.Do(func() {
		client, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{host},
			DialTimeout: 5 * time.Second,
		})
		if err != nil {
			panic(err)
		}
		serviceRegistry = &ServiceRegistry{
			client:    client,
			serviceId: serviceId,
		}
	})
}

func Client() *ServiceRegistry {
	return serviceRegistry
}

func (s *ServiceRegistry) Start() {
	for {
		value := "ready"
		ttl := 15 // 60 seconds

		// Create a lease with the desired TTL
		leaseResp, err := s.client.Grant(context.Background(), int64(ttl))
		if err != nil {
			fmt.Println(err)
			return
		}

		_, err = s.client.Put(context.Background(), fmt.Sprintf(serviceKeyTemplate, s.serviceId), value, clientv3.WithLease(leaseResp.ID))
		if err != nil {
			fmt.Println(err)
			return
		}
		time.Sleep(time.Second * 10)
	}
}

func (s *ServiceRegistry) PublishEvent(event string) {
	ttl := 15 // 60 seconds

	// Create a lease with the desired TTL
	leaseResp, err := s.client.Grant(context.Background(), int64(ttl))
	if err != nil {
		fmt.Println(err)
		return
	}

	_, err = s.client.Put(context.Background(), fmt.Sprintf(serviceKeyTemplate, s.serviceId), event, clientv3.WithLease(leaseResp.ID))
	if err != nil {
		fmt.Println(err)
		return
	}
}
