package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/segmentio/kafka-go"

	"github.com/mailru/easyjson"
	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
)

type Config struct {
	KafkaWorkers  int    `json:"kafka_workers"`
	ClickWorkers  int    `json:"click_workers"`
	Brokers       string `json:"brokers"`
	ClickServers  string `json:"click_servers"`
	Topic         string `json:"topic"`
	GroupID       string `json:"group_id"`
	BatchSize     int    `json:"batch_size"`
	FlushTime     int    `json:"flush_time"`
	WaitTime      int    `json:"wait_time"`
	Regexp        string `json:"regexp"`
	User          string `json:"user"`
	Password      string `json:"password"`
	Table         string `json:"table"`
	PartitionFrom int    `json:"partition_from"`
	PartitionTo   int    `json:"partition_to"`
}

var (
	kafkaWorkers int
	clickWorkers int
	_            *json.RawMessage
	_            *jlexer.Lexer
	_            *jwriter.Writer
	_            easyjson.Marshaler
)

const EmptyBatch = 5000

type KafkaReader struct {
	ID      int
	conn    *kafka.Conn
	reader  *kafka.Reader
	End     chan bool
	Trigger chan bool
}

type KafkaBeat struct {
	Hostname, Timezone string
}

// easyjson:json
type KafkaMessage struct {
	Message string
	Beat    KafkaBeat
}

type ClickMessage struct {
	StoreID           string
	POSID             uint16
	MsgDate           time.Time
	EventDate         time.Time
	TimeMS            uint16
	Severity, Context string
	Thread, ClassName string
	LogMsg, TZLocal   string
	TZHours           float32
	ProcessingDate    time.Time
	ProcessingDateMS  uint16
}

type ClickSync struct {
	ID       int
	End      chan bool
	User     string
	Password string
}

type parseElements struct {
	posIDint  int64
	eventTxt  string
	timeMSint int64
	timeHours int
	timeNow   string
	diffToMSK int
}

func checkInfo(mark string, err error) {
	if err != nil {
		log.Println("ERR", mark, ":", err)
	}
}

func checkFatal(mark string, err error) {
	if err != nil {
		log.Fatal("ERR", mark, ":", err)
	}
}

// UnmarshalJSON supports json.Unmarshaler interface
func (v *KafkaMessage) UnmarshalJSON(data []byte) error {
	r := jlexer.Lexer{Data: data}
	easyjsonDecoder(&r, v)
	return r.Error()
}

func easyjsonDecoder(in *jlexer.Lexer, out *KafkaMessage) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeFieldName(false)
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "message":
			out.Message = in.String()
		case "beat":
			easyjsonBeatDecoder(in, &out.Beat)
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}

func easyjsonBeatDecoder(in *jlexer.Lexer, out *KafkaBeat) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeFieldName(false)
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		switch key {
		case "hostname":
			out.Hostname = in.String()
		case "timezone":
			out.Timezone = in.String()
		default:
			in.SkipRecursive()
		}
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}

func (kr *KafkaReader) startRead(config *Config, out chan []byte) {

	log.Printf("KafkaReader [%d] started", kr.ID)

	dialer := &kafka.Dialer{
		Timeout:   10 * time.Second,
		DualStack: true,
	}

	var partition = kr.ID + config.PartitionFrom
	var m kafka.Message

	// 0-partition leader searching
	conn, err := dialer.DialLeader(context.Background(), "tcp",
		":9092", config.Topic, 0)
	checkFatal("dial 0-leader failed", err)
	// now we have a list with partitions and leaders
	partsSlice, err := conn.ReadPartitions()

	// now we have a connection with the leader of a partition we needed
	conn, err = dialer.DialLeader(context.Background(), "tcp",
		partsSlice[kr.ID].Leader.Host, config.Topic, kr.ID)
	checkFatal("dial #ID leader failed", err)

	for {
		select {
		case <-kr.End:
			func(c *kafka.Conn) {
				err = c.Close()
			}(conn)
			return

		// one partition - one reader
		case <-kr.Trigger:
			partition += kafkaWorkers
			if partition >= config.PartitionTo {
				partition = kr.ID + config.PartitionFrom
			}
			log.Printf("Next partition on ID[%d]: %d", kr.ID, partition)
			conn, err = dialer.DialLeader(context.Background(),
				"tcp", partsSlice[partition].Leader.Host, config.Topic, partition)

		default:
			var batch *kafka.Batch
			c1 := make(chan *kafka.Batch, 1)

			go func() {
				c1 <- conn.ReadBatch(1e3, 1e9)
			}()

			select {
			case batch = <-c1:
				close(c1)
				var counter int
				for {
					m, err = batch.ReadMessage()
					if err != nil {
						break
					}
					out <- m.Value
					counter++
				}
				// if we got less than 5k batch
				// it's time to change partition
				if counter < EmptyBatch {
					kr.Trigger <- true
				}
				_ = batch.Close()
			case <-time.After(20 * time.Second):
				kr.Trigger <- true
			}
		}
	}
}

func (sync *ClickSync) clickInsert(config *Config, ch chan []byte, server string) {

	var (
		intBase      = 10
		intSize      = 16
		data         []byte
		kmsg         KafkaMessage
		clickMessage ClickMessage
		elements     parseElements
	)

	msgslice := make([]ClickMessage, 0, config.BatchSize)
	ticker := time.NewTicker(time.Duration(config.FlushTime) * time.Second)

	for {
		select {
		case <-sync.End:
			sync.pushToClick(config, msgslice, server)
			return

		case <-ticker.C:
			go sync.pushToClick(config, msgslice, server)
			msgslice = msgslice[:0]

		case data = <-ch:
			err := kmsg.UnmarshalJSON(data)
			data = nil
			if err != nil {
				continue
			}
			// empty message
			if kmsg.Message == "" {
				continue
			}
			// if not first 2 in message
			if kmsg.Message[0] != 50 {
				continue
			}
			fixedParts := strings.SplitN(kmsg.Message, " ", 4)
			if len(fixedParts) < 4 {
				continue
			}
			contextPart := strings.SplitAfter(fixedParts[3], ">")
			if len(contextPart) < 2 {
				continue
			}
			threadPart := strings.SplitAfter(contextPart[1][1:], "}")
			if len(threadPart) < 2 {
				continue
			}
			classPart := strings.SplitAfter(threadPart[1][1:], "]")
			if len(classPart) < 2 {
				continue
			}
			clickMessage.Severity = fixedParts[2]
			clickMessage.Context = contextPart[0]
			clickMessage.Thread = threadPart[0]
			clickMessage.ClassName = classPart[0]
			clickMessage.LogMsg = classPart[1][1:]
			contextPart = nil
			threadPart = nil
			classPart = nil

			clickMessage.MsgDate, _ = time.Parse("2006-01-02", fixedParts[0])
			elements.eventTxt = fixedParts[0] + " " + fixedParts[1][:8]
			clickMessage.EventDate, _ = time.Parse("2006-01-02 15:04:05", elements.eventTxt)
			elements.timeMSint, _ = strconv.ParseInt(fixedParts[1][9:], intBase, intSize)
			fixedParts = nil

			posParts := strings.Split(kmsg.Beat.Hostname, "-")
			clickMessage.StoreID = posParts[2]
			elements.posIDint, _ = strconv.ParseInt(posParts[0][3:], intBase, intSize)
			clickMessage.POSID = uint16(elements.posIDint)
			posParts = nil

			clickMessage.TimeMS = uint16(elements.timeMSint)
			clickMessage.ProcessingDateMS = clickMessage.TimeMS

			clickMessage.TZLocal = "GMT" + kmsg.Beat.Timezone
			elements.timeHours, _ = strconv.Atoi(kmsg.Beat.Timezone[1:3])
			clickMessage.TZHours = float32(elements.timeHours)
			elements.timeNow = time.Now().Format("2006-01-02 15:04:05")
			clickMessage.ProcessingDate, _ = time.Parse("2006-01-02 15:04:05", elements.timeNow)
			elements.diffToMSK = elements.timeHours - 3
			clickMessage.EventDate = clickMessage.EventDate.Add(time.Duration(-1*elements.diffToMSK) * time.Hour)

			msgslice = append(msgslice, clickMessage)
			if len(msgslice) >= config.BatchSize {
				go sync.pushToClick(config, msgslice, server)
				msgslice = msgslice[:0]
			}
		}
	}
}

func (sync *ClickSync) pushToClick(config *Config, data []ClickMessage, server string) {

	connect, err := sql.Open("clickhouse", server)
	checkFatal("connect to Click", err)
	defer func(s *sql.DB) {
		err := s.Close()
		checkInfo("closing Click", err)
	}(connect)

	defer func(data []ClickMessage) {
		data = nil
	}(data)

	if len(data) > 0 {
		log.Printf("Saving %d to Click", len(data))
	} else {
		log.Println("No data to save")
		return
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			checkInfo("checkalive Click", err)
		}
		return
	}

	query := fmt.Sprintf("INSERT INTO %s (STORE_ID, POS_ID, MSG_DATE, EVENT_DATE, "+
		"TIME_MS, SEVERITY, CONTEXT, THREAD, CLASS_NAME, LOG_MSG, TZ_LOCAL, TZ_HOURS, "+
		"PROCESSING_DATE, PROCESSING_DATE_MS) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", config.Table)
	var (
		tx, _   = connect.Begin()
		stmt, _ = tx.Prepare(query)
	)
	defer func(s *sql.Stmt) {
		err := s.Close()
		checkInfo("closing Click", err)
	}(stmt)

	for _, row := range data {
		_, err = stmt.Exec(
			row.StoreID,
			row.POSID,
			row.MsgDate,
			row.EventDate,
			row.TimeMS,
			row.Severity,
			row.Context,
			row.Thread,
			row.ClassName,
			row.LogMsg,
			row.TZLocal,
			row.TZHours,
			row.ProcessingDate,
			row.ProcessingDateMS,
		)
		checkInfo("Click statement exec", err)
	}

	if err := tx.Commit(); err != nil {
		checkFatal("commit to Click", err)
	}
}

func (sync *ClickSync) stop() {
	log.Printf("ClickSync [%d] is stopping", sync.ID)
	sync.End <- true
}

func (kr *KafkaReader) stop() {
	log.Printf("KafkaReader [%d] is stopping", kr.ID)
	kr.End <- true
}

// click nodes round-robin
func roundClick(config *Config) []string {
	names := strings.Split(config.ClickServers, ";")
	times := int(math.Ceil(float64(config.ClickWorkers) / float64(len(names))))
	var servers []string
	for i := 0; i < times; i++ {
		for _, name := range names {
			servers = append(servers,
				fmt.Sprintf("tcp://%s?user=&password=", name))
		}
	}
	fmt.Println(servers)
	return servers
}

func main() {
	file, err := os.Open("config.json")
	checkFatal("open config", err)
	defer func(f *os.File) {
		err := f.Close()
		checkInfo("closing config", err)
	}(file)
	decoder := json.NewDecoder(file)
	config := Config{}
	err = decoder.Decode(&config)
	checkFatal("config decode", err)

	kafkaData := make(chan []byte, 128000)
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, syscall.SIGINT, syscall.SIGTERM)

	kafkaWorkers = config.KafkaWorkers
	clickWorkers = config.ClickWorkers

	var kafkaReaders []*KafkaReader
	for i := 0; i < kafkaWorkers; i++ {
		kr := KafkaReader{
			ID:      i,
			End:     make(chan bool, 1),
			Trigger: make(chan bool, 1)}
		kafkaReaders = append(kafkaReaders, &kr)
		go kr.startRead(&config, kafkaData)
	}

	var chSyncers []*ClickSync
	connString := roundClick(&config)
	for i := 0; i < clickWorkers; i++ {
		chs := ClickSync{
			ID:       i,
			End:      make(chan bool, 1),
			User:     config.User,
			Password: config.Password}
		chSyncers = append(chSyncers, &chs)
		go chs.clickInsert(&config, kafkaData, connString[i])
	}

	for {
		s := <-signalCh
		log.Printf("Got signal: %s.. stopping", s)
		for _, kr := range kafkaReaders {
			kr.stop()
		}
		for _, sync := range chSyncers {
			sync.stop()
		}
		time.Sleep(5 * time.Second)
		os.Exit(0)
	}
}
