package startup

import (
	"errors"
	kuiperapi "github.com/c12s/kuiper/pkg/api"
	magnetarapi "github.com/c12s/magnetar/pkg/api"
	"github.com/c12s/star/internal/configs"
	"github.com/c12s/star/internal/servers"
	"github.com/c12s/star/internal/services"
	"github.com/c12s/star/internal/store"
	"github.com/c12s/star/pkg/api"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

type app struct {
	config            *configs.Config
	grpcServer        *grpc.Server
	configAsyncServer *servers.ConfigAsyncServer
	shutdownProcesses []func()
	serfAgent         *services.SerfAgent
}

func NewAppWithConfig(config *configs.Config) (*app, error) {
	if config == nil {
		return nil, errors.New("config is nil")
	}
	return &app{
		config:            config,
		shutdownProcesses: make([]func(), 0),
	}, nil
}

func (a *app) init() {
	natsConn, err := NewNatsConn(a.config.NatsAddress())
	if err != nil {
		log.Fatalln(err)
	}
	a.shutdownProcesses = append(a.shutdownProcesses, func() {
		log.Println("closing nats conn")
		natsConn.Close()
	})

	nodeIdStore, err := store.NewNodeIdFSStore(a.config.NodeIdDirPath(), a.config.NodeIdFileName())
	if err != nil {
		log.Fatalln(err)
	}

	registrationClient, err := magnetarapi.NewRegistrationAsyncClient(a.config.NatsAddress())
	if err != nil {
		log.Fatalln(err)
	}

	registrationService := services.NewRegistrationService(registrationClient, nodeIdStore)
	if !registrationService.Registered() {
		err := registrationService.Register(a.config.MaxRegistrationRetries())
		if err != nil {
			log.Fatalln(err)
		}
	}

	nodeId, err := nodeIdStore.Get()
	if err != nil {
		log.Fatalln(err)
	}
	log.Println("NODE ID: " + nodeId.Value)

	configStore, err := store.NewConfigInMemStore()
	if err != nil {
		log.Fatalln(err)
	}

	configClient, err := kuiperapi.NewKuiperAsyncClient(a.config.NatsAddress(), nodeId.Value)
	if err != nil {
		log.Fatalln(err)
	}
	configAsyncServer, err := servers.NewConfigAsyncServer(configClient, configStore)
	if err != nil {
		log.Fatalln(err)
	}
	a.configAsyncServer = configAsyncServer

	configGrpcServer, err := servers.NewStarConfigServer(configStore)
	if err != nil {
		log.Fatalln(err)
	}

	s := grpc.NewServer()
	api.RegisterStarConfigServer(s, configGrpcServer)
	reflection.Register(s)
	a.grpcServer = s

	agent, err := services.NewSerfAgent(a.config, natsConn, nodeId.Value)
	a.serfAgent = agent
}

func (a *app) startSerfAgent() error {
	err := a.serfAgent.Join(true)
	if err != nil {
		return err
	}
	a.serfAgent.RunMock()
	//a.serfAgent.RunMock2()
	a.serfAgent.Wg.Add(1)
	go a.serfAgent.Listen()
	go a.serfAgent.ListenNATS()
	return nil
}

func (a *app) startConfigAsyncServer() error {
	a.configAsyncServer.Serve()
	return nil
}

func (a *app) startGrpcServer() error {
	lis, err := net.Listen("tcp", a.config.GrpcServerAddress())
	if err != nil {
		return err
	}
	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := a.grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	return nil
}

func (a *app) Start() error {
	a.init()

	err := a.startConfigAsyncServer()
	if err != nil {
		return err
	}
	err = a.startGrpcServer()
	if err != nil {
		return err
	}
	err = a.startSerfAgent()
	if err != nil {
		return err
	}
	return nil
}

func (a *app) GracefulStop() {
	go a.configAsyncServer.GracefulStop()
	a.grpcServer.GracefulStop()
	a.serfAgent.Leave()
	for _, shudownProcess := range a.shutdownProcesses {
		shudownProcess()
	}
}
