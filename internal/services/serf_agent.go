package services

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"github.com/c12s/star/internal/configs"
	"github.com/hashicorp/serf/serf"
	nats "github.com/nats-io/nats.go"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SerfAgent payloadBacklog is needed only if the  payload splitting option is used
type SerfAgent struct {
	agent          *serf.Serf
	config         *serf.Config
	eventChannel   chan serf.Event
	stopChannel    chan struct{}
	configs        *configs.Config
	Wg             sync.WaitGroup
	nc             *nats.Conn
	payloadBacklog map[string]string
	nodeId         string
}

// NewSerfAgent payloadBacklog is needed only if the  payload splitting option is used
func NewSerfAgent(cf *configs.Config, nc *nats.Conn, nodeId string) (*SerfAgent, error) {
	serfConfig := serf.DefaultConfig()
	serfChannel := make(chan serf.Event)
	tags, err := createTags(cf.SerfTag())
	if err != nil {
		log.Fatal(err)
	}
	serfConfig.EventCh = serfChannel
	serfConfig.NodeName = cf.SerfNodeName()
	serfConfig.Tags = tags
	serfConfig.MemberlistConfig.BindAddr = cf.SerfBindAddress()
	serfConfig.MemberlistConfig.BindPort = cf.SerfBindPort()
	agent, err := serf.Create(serfConfig)
	if err != nil {
		log.Fatal(err)
	}
	stopChannel := make(chan struct{}) // Stop channel
	return &SerfAgent{
		agent:          agent,
		config:         serfConfig,
		eventChannel:   serfChannel,
		stopChannel:    stopChannel,
		configs:        cf,
		Wg:             sync.WaitGroup{},
		nc:             nc,
		payloadBacklog: make(map[string]string),
		nodeId:         nodeId,
	}, nil
}

func (s *SerfAgent) Join(ignoreOld bool) error {
	_, err := s.agent.Join([]string{s.configs.SerfJoinAddress() + ":" + s.configs.SerfJoinPort()}, ignoreOld)
	if err != nil {
		return err
	}
	return nil
}

func (s *SerfAgent) Leave() {
	close(s.stopChannel)
	s.Wg.Wait()
	err := s.agent.Leave()
	if err != nil {
		log.Println(err)
		panic(err)
	}
}

func (s *SerfAgent) Listen() {
	defer s.Wg.Done()
	for {
		select {
		case event := <-s.eventChannel:
			handleEvents(event, s)
		case <-s.stopChannel:
			fmt.Println("Leaving Cluster")
			return
		}
	}
}

// ListenNATS subject in comment is for troubleshooting
// starts the NATS message listener
func (s *SerfAgent) ListenNATS() {
	//subject := "configs"
	subject := fmt.Sprintf("configs.%s", s.nodeId)
	_, err := s.nc.Subscribe(subject, func(msg *nats.Msg) {
		s.handleNATSMessage(msg.Data)
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("NATS Subscribed to %s", subject)

}

// handleNATSMessage
// TriggerUserEvent is used for default serf max payload size of 512b
// TriggerUserEventSplit is used for extending the message size, with some added logic for payload splitting and reassembly
func (s *SerfAgent) handleNATSMessage(data []byte) {
	msg := string(data)
	values := strings.Split(msg, "|")
	tags := values[0]
	coalesce, err := strconv.ParseBool(values[1])
	if err != nil {
		log.Println(err)
	}
	payload := values[2]
	log.Println("nats message had info: ", tags, payload, coalesce)

	// Test Block
	//test, err := s.checkTags(tags)
	//if err != nil {
	//	log.Println(err)
	//}
	//if test {
	//	err = s.TriggerUserEvent(tags, payload, coalesce)
	//	//err = s.TriggerUserEventSplit(tags, payload, coalesce)
	//	log.Println("Mocking user event:", tags)
	//
	//}

	err = s.TriggerUserEvent(tags, payload, coalesce)
	//err = s.TriggerUserEventSplit(tags, payload, coalesce)
	if err != nil {
		log.Println(err)
	}
}

// TriggerUserEvent is used for no payload splitting
func (s *SerfAgent) TriggerUserEvent(name, payload string, coalesce bool) error {
	payloadBytes, err := preparePayload(payload)
	if err != nil {
		return err
	}
	if len(payloadBytes) > 512 {
		log.Println("Payload exceeds maximum size, aborting UserEvent")
		return nil
	}
	err = s.agent.UserEvent(name, payloadBytes, coalesce)
	if err != nil {
		return err
	}
	return nil
}

func (s *SerfAgent) GetClusterMembers() []serf.Member {
	return s.agent.Members()
}

// handleEvents
// handleUserEvent is used for default serf max payload size of 512b
// handleUserEventSplit is used for extending the message size, with some added logic for payload splitting and reassembly
func handleEvents(ev serf.Event, s *SerfAgent) {
	switch ev.EventType() {
	case serf.EventMemberJoin:
		handleMemberJoin(ev)
	case serf.EventMemberLeave:
		handleMemberLeave(ev)
	case serf.EventMemberFailed:
		handleMemberFailed(ev)
	case serf.EventMemberUpdate:
		handleMemberUpdate(ev)
	case serf.EventMemberReap:
		handleMemberReap(ev)
	case serf.EventUser:
		handleUser(ev, s)
		//handleUserSplit(ev, s)
	case serf.EventQuery:
		handleQuery(ev)
	default:
		log.Printf("Unknown event: %v, no case defined", ev.EventType())
	}
}

func handleMemberJoin(ev serf.Event) {
	log.Println("MemberJoinEvent handled:", ev.EventType())
}
func handleMemberLeave(ev serf.Event) {
	log.Println("MemberLeaveEvent handled:", ev.EventType())
}
func handleMemberFailed(ev serf.Event) {
	log.Println("MemberFailedEvent handled:", ev.EventType())
}
func handleMemberUpdate(ev serf.Event) {
	log.Println("MemberUpdateEvent handled:", ev.EventType())
}
func handleMemberReap(ev serf.Event) {
	log.Println("MemberReadEvent handled:", ev.EventType())
}
func handleQuery(ev serf.Event) {
	log.Println("QueryEvent handled:", ev.EventType())
}

// No payload splitting

// handleUser is used for no payload splitting
func handleUser(ev serf.Event, s *SerfAgent) {
	log.Println("UserEvent handled:", ev.EventType())
	if ue, ok := ev.(serf.UserEvent); ok {
		var payload string
		log.Printf("Event name: %s  Event coalescing: %t", ue.Name, ue.Coalesce)
		tag := ue.Name
		intended, err := s.checkTags(tag)
		if err != nil {
			log.Println(err)
		}
		if intended {
			payload, err = parsePayload(ue.Payload)
			log.Printf("Payload for this node:\n %s", payload)
		} else {
			log.Println("Payload not for this node")
		}
	} else {
		log.Println("Failed to cast to UserEvent")
	}
}

// createTags adds the config tags to the serf agent
func createTags(s string) (map[string]string, error) {
	tags := make(map[string]string)
	tagList := strings.Split(s, ",")
	for _, v := range tagList {
		if pair := strings.Split(v, ":"); len(pair) == 2 {
			tags[pair[0]] = pair[1]
		} else {
			return nil, errors.New("wrong tag format")
		}
	}
	return tags, nil
}

// checkTags checks if the serf UserEvent is intended for the serf agent
// only 1 KV pair can be used for the check
func (s *SerfAgent) checkTags(tag string) (bool, error) {
	if tagPair := strings.Split(tag, ":"); len(tagPair) == 2 {
		if val, ok := s.config.Tags[tagPair[0]]; ok {
			return val == tagPair[1], nil
		}
		return false, nil
	}
	return false, errors.New("wrong tag format")
}

// preparePayload compresses the payload to save space, slightly increasing the max size of the payload (dependant on the compression)
// will still cause errors if the compressed payload is over 512b
func preparePayload(payload string) ([]byte, error) {
	data := []byte(payload)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write(data)
	if err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// parsePayload is reads the compressed data, turns it back to string format
func parsePayload(payload []byte) (string, error) {
	r, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return "", err
	}
	defer func(r *gzip.Reader) {
		err := r.Close()
		if err != nil {
		}
	}(r)
	val, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// RunMock Test 1
// First block used for no payload splitting
// Second block used for NATS
// Third block used for payload splitting
func (s *SerfAgent) RunMock() {
	if s.configs.SerfNodeName() == "star_1" {
		time.Sleep(20 * time.Second)

		//Block 1
		payload := "\n payload \n payload \n payload \n payload \n payload \n payload \n payload \n payload "
		userEventName := "tag1:type_3"
		time.Sleep(15 * time.Second)
		log.Println("Mocking user event:", userEventName)
		err := s.TriggerUserEvent(userEventName, payload, true)
		if err != nil {
			log.Fatalln(err)
		}
		log.Printf("UserEvent triggered--------------------------- %s \n", userEventName)

		//BLock 2
		//subject := "configs"
		//subject := fmt.Sprintf("configs.%s", s.nodeId)
		//s.nc.Publish(fmt.Sprintf(subject), []byte("tag1:type_2|true|payload je ovo"))
		//log.Println("published event on nats: ", subject)

		//Block 3
		//payload2 := "\n                                         ..............                                          \n                               ...:::----====================----::...                              \n                        ..::-==========-------------------------=======--:.       ...               \n          ..:::::.. ..-======------------------------------==========---====-::-=======--.          \n      .:==========++=+=--=======---======-------------====--::....::--===---==**=------==++-        \n     -++=-------=++=--=+=-:..        ..:-++=--------=+=:.              .-+=----=+*##*=-----*+.      \n    -*----*%@@%*=----*=.                  :++------*=. .::.              .++------*@@%------#:      \n    *=---=%@@@+-----*- :+*##*:.            .*=----++ .*%@@@#-             .#-------=#+-----=#:      \n    :*=----+%=------# .%@@@%#@-             ++----*= -@@@@+##              #=--------*=--=++:       \n     .-====#=-------*: =%@@#++.            .*=-----*-.-*##*=.            .=+---------=#=--.         \n        .:*+---------*-...::.            .-+=---====+=:.              .:-+=-----------++            \n         :#-----------=+=:..        ..::===-+#%@@@@@%#+==--::....:::-====--------------#:           \n         ++--------------=====-----========+@@@@@@@@@%*==============------------------++           \n         *=----------------------------=+-:::-=+++==-:::-=+=----------------------------#.          \n        .#-----------------------------++:::::::---:::::::=*----------------------------#:          \n        .#------------------------------==++---:**::-=+==++-----------------------------*=          \n        .#=-------------------------------+*    #=   -#---------------------------------++          \n         *=--------------------------------*-.:-#+:.:=*---------------------------------++          \n         ++---------------------------------====-=====----------------------------------+*          \n         -*-----------------------------------------------------------------------------=*          \n         :#-----------------------------------------------------------------------------=*          \n         .#=----------------------------------------------------------------------------=*          \n          *=----------------------------------------------------------------------------=*          \n          =+----------------------------------------------------------------------------=*          \n    ..:::-+*----------------------------------------------------------------------------=#---::.    \n .:===---:+*-----------------------------------------------------------------------------#-:---==:. \n -#=-:::::+*-----------------------------------------------------------------------------#=::::-+#. \n .-*=--==-*+-----------------------------------------------------------------------------*+-==-=*.  \n   .::..  +=-----------------------------------------------------------------------------++ ....    \n         .*------------------------------------------------------------------------------+*         \n         :#------------------------------------------------------------------------------=*         \n         -*------------------------------------------------------------------------------=#.        \n         ++-------------------------------------------------------------------------------#.        \n         +=------------------------------------------------------------------------------=#.        \n         *=------------------------------------------------------------------------------=*         \n         *=------------------------------------------------------------------------------++         \n         ++------------------------------------------------------------------------------#-         \n         -*-----------------------------------------------------------------------------=#.         \n         .#-----------------------------------------------------------------------------#-          \n          =+---------------------------------------------------------------------------*+           \n          .+=-------------------------------------------------------------------------++.           \n           .++-----------------------------------------------------------------------*=.            \n            .=+=-------------------------------------------------------------------=+-              \n              :=+=---------------------------------------------------------------=+-.               \n                .=*+=--------------------------------------------------------==++*-.                \n             .:-==--=++==------------------------------------------------==+==-:::=+=.              \n            :+---::::::-+**=====----------------------------------======--=+=-::::=:-*:             \n            =+=+-:::-==-:....::---==============================---::..     :==-::-*=*:             \n            .-+=--==-:.            ......::::::::::::::::......               .:---=-.              \n               ....                                                                               "
		//userEventName2 := "tag2:val2"
		//time.Sleep(15 * time.Second)
		//log.Println("Mocking user event:", userEventName2)
		//err = s.TriggerUserEventSplit(userEventName2, payload2, true)
		//if err != nil {
		//	log.Fatalln(err)
		//}
		//log.Printf("UserEvent triggered--------------------------- %s \n", userEventName2)
	}
}

// RunMock2 Test 2
// Leaving the cluster
//func (s *SerfAgent) RunMock2() {
//	if s.configs.SerfNodeName() == "star_1" {
//		log.Println("members before leave: ", s.GetClusterMembers())
//		time.Sleep(10 * time.Second)
//		s.Leave()
//		time.Sleep(10 * time.Second)
//		log.Println("members after leave: ", s.GetClusterMembers())
//	}
//}

// Payload splitting

// constructName used for generating a UserEvent name with additional splitting info
func constructName(chunk, max int, name string) string {
	return fmt.Sprintf("%s|%d|%d", name, chunk, max)
}

// chunkPayload used for splitting the initial string value into substrings for later compression
// not space efficient because the compression is done after the payload is split into chunks under 512b
// splitting after compression requires additional logic for later reassembly
func chunkPayload(b string) ([]string, int) {
	size := 450
	if len(b) > size {
		retInt := len(b)/size + 1
		var chunks []string
		for i := 0; i < len(b); i += size {
			end := i + size
			if end > len(b) {
				end = len(b)
			}
			chunks = append(chunks, b[i:end])
		}
		return chunks, retInt
	}
	return []string{b}, 1
}

// checkChunks checks if the entire message is present in the agent
// if the entire message is present will trigger reassembly
func (s *SerfAgent) checkChunks(full int) bool {
	if len(s.payloadBacklog) == full {
		return true
	}
	return false
}

// reassemblePayload constructs the payload from chunks stored in payloadBacklog
func (s *SerfAgent) reassemblePayload(fullInt int) string {
	var full string
	for i := 0; i < fullInt; i++ {
		key := strconv.Itoa(i)
		log.Println(s.payloadBacklog[key])
		full += s.payloadBacklog[key]
	}
	return full
}

// TriggerUserEventSplit used to trigger events with additional payload splitting logic
// increases max size of payload beyond 512b, sends multiple events
// payloads get reassembled when all the data is present in the payloadBacklog
func (s *SerfAgent) TriggerUserEventSplit(name, payload string, coalesce bool) error {
	chunks, num := chunkPayload(payload)
	for ind, val := range chunks {
		value, err := preparePayload(val)
		err = s.agent.UserEvent(constructName(ind+1, num, name), value, coalesce)
		if err != nil {
			return err
		}
	}
	return nil
}

// handleUserSplit used for handling events when payload splitting option is chosen
// will check if entire message is present in the payloadBacklog after every event it handles
func handleUserSplit(ev serf.Event, s *SerfAgent) {
	log.Println("UserEvent handled:", ev.EventType())
	if ue, ok := ev.(serf.UserEvent); ok {
		var payload string
		log.Printf("Event name: %s  Event coalescing: %t", ue.Name, ue.Coalesce)
		tags := strings.Split(ue.Name, "|")
		intended, err := s.checkTags(tags[0])
		if err != nil {
			log.Println(err)
		}
		if intended {
			fullInt, err := strconv.Atoi(tags[2])
			if err != nil {
				log.Println(err)
			}
			if fullInt > 1 {
				payload, err = parsePayload(ue.Payload)
				log.Println(tags[1], payload)
				s.payloadBacklog[tags[1]] = payload
				if s.checkChunks(fullInt) {
					payload = s.reassemblePayload(fullInt)
					log.Printf("Payload for this node:\n %s", payload)
					s.payloadBacklog = nil
					s.payloadBacklog = make(map[string]string)
				} else {
					log.Println("Missing parts of payload")
				}
			} else {
				payload, err = parsePayload(ue.Payload)
				log.Printf("Payload for this node:\n %s", payload)
			}
		} else {
			log.Println("Payload not for this node")
		}

	} else {
		log.Println("Failed to cast to UserEvent")
	}
}
