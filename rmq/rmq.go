package rmq

import (
	"context"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/streadway/amqp"
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.WarnLevel)
}

type Config struct {
	HostName               string
	UserName               string
	Password               string
	VirtualHost            string
	Port                   int
	FailedRetryInterval    int
	FailedRetryCount       int
	ExchangeName           string
	ExchangeType           string
	QueueName              string //默认队列名同 routing key
	DeadLetterExchangeName string
	DeadLetterRoutingKey   string
	DelaySeconds           int64
	Action                 string
}

type Message struct {
	amqp.Delivery
}

const (
	reInitDelay = time.Second

	actionPub      = 1
	actionSub      = 2
	actionDelay    = 3
	actionPubStr   = "pub"
	actionSubStr   = "sub"
	actionDelayStr = "delay"

	addrFmt = "amqp://%s:%s@%s:%d/"

	rmqTrace   = "rmq.trace"
	traceMqOpt = "mq consumer"

	msgContentType = "text/plain"
)

var (
	ErrMQConfig            = errors.New("config is err")
	ErrInitConnect         = errors.New("client connect err")
	ErrInitChannel         = errors.New("client channel init err")
	ErrNotConnected        = errors.New("client connection is not ready")
	ErrNotReadyChannel     = errors.New("client channel is not ready")
	ErrShutdown            = errors.New("client is shutting down")
	ErrNotSupportOperation = errors.New("client not support operation")
)

type Client struct {
	conf      *Config
	dsn       string
	action    int
	delayArgs amqp.Table
	tid       string

	isConnected    *atomic.Bool //conn
	isChannelReady *atomic.Bool //channel
	isClosed       *atomic.Bool //closed

	connection      *amqp.Connection
	channel         *amqp.Channel
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
}

func NewProducer(conf *Config) (*Client, error) {
	return newRabbitClient(actionPub, conf)
}

func NewDelayProducer(conf *Config) (*Client, error) {
	return newRabbitClient(actionDelay, conf)
}
func NewConsumer(conf *Config) (*Client, error) {
	return newRabbitClient(actionSub, conf)
}

func newRabbitClient(action int, conf *Config) (resp *Client, err error) {

	if conf == nil {
		log.Error("newRabbitClient conf is nil")
		return nil, ErrMQConfig
	}

	if isEmpty(conf.UserName) || isEmpty(conf.Password) || isEmpty(conf.HostName) ||
		isEmpty(conf.ExchangeType) || isEmpty(conf.ExchangeName) || isEmpty(conf.QueueName) ||
		conf.Port <= 0 || conf.FailedRetryCount <= 0 {
		log.Error("newRabbitClient conf invalid %+v", conf)
		return nil, ErrMQConfig
	}

	cli := &Client{
		conf:           conf,
		action:         action,
		dsn:            fmt.Sprintf(addrFmt, conf.UserName, conf.Password, conf.HostName, conf.Port),
		tid:            fmt.Sprintf("queue: %s,action: %s ", conf.QueueName, conf.Action),
		isConnected:    atomic.NewBool(false),
		isChannelReady: atomic.NewBool(false),
		isClosed:       atomic.NewBool(false),
	}

	switch action {
	case actionPub:
		if !isPubAction(conf.Action) {
			log.Error("newRabbitClient action invalid %v", conf.Action)
			return nil, ErrMQConfig
		}

	case actionSub:
		if !isSubAction(conf.Action) {
			log.Error("newRabbitClient action invalid %v", conf.Action)
			return nil, ErrMQConfig
		}

	case actionDelay:
		if !isDelayAction(conf.Action) {
			log.Error("newRabbitClient action invalid %v", conf.Action)
			return nil, ErrMQConfig
		}
		if conf.DelaySeconds <= 0 || isEmpty(conf.DeadLetterExchangeName) || isEmpty(conf.DeadLetterRoutingKey) {
			log.Error("newRabbitClient conf invalid %+v", conf)
			return nil, ErrMQConfig
		}
		cli.delayArgs = amqp.Table{
			"x-message-ttl":             conf.DelaySeconds * 1000,
			"x-dead-letter-exchange":    conf.DeadLetterExchangeName,
			"x-dead-letter-routing-key": conf.DeadLetterRoutingKey,
		}
	}

	err = cli.initConnect()
	if err != nil {
		log.Error("newRabbitClient ErrInitConnect %v", err)
		return nil, ErrInitConnect
	}

	err = cli.initChannel()
	if err != nil {
		log.Error("newRabbitClient ErrInitChannel %v", err)
		return nil, ErrInitChannel
	}
	resp = cli
	log.Info("newRabbitClient success %s", resp.tid)
	return
}

func (s *Client) initConnect() error {

	s.isConnected.Store(false)
	conn, err := amqp.Dial(s.dsn)
	if err != nil {
		log.Error("initConnect %v", err)
		return err
	}

	s.changeConnection(conn)
	s.isConnected.Store(true)
	go s.handleReconnect()
	log.Info("initConnect success! %s", s.tid)
	return nil
}

func (s *Client) changeConnection(conn *amqp.Connection) {
	s.connection = conn
	s.notifyConnClose = make(chan *amqp.Error)
	s.connection.NotifyClose(s.notifyConnClose)
}

func (s *Client) handleReconnect() {

	time.Sleep(time.Second)
	for range s.notifyConnClose {
		log.Error("handleReconnect notifyConnClose. Reconnecting... %s", s.tid)
		time.Sleep(reInitDelay)
		try := 0

	ReTry:

		try++
		if s.isClosed.Load() {
			log.Warn("handleReconnect isClosed. %s, %v ", s.tid, ErrShutdown)
			return
		}
		log.Warn("handleReconnect attempting to connect %s,try %d", s.tid, try)
		err := s.initConnect()
		if err != nil {
			log.Error("handleReconnect failed to connect. Retrying...%s, try:%d, err:%v", s.tid, try, err)
			time.Sleep(tryIntervalDuration(try, s.conf.FailedRetryCount))
			goto ReTry
		}
		return
	}
}

// init will initialize channel & declare exchange & queue
func (s *Client) initChannel() (err error) {
	s.isChannelReady.Store(false)
	channel, err := s.connection.Channel()
	if err != nil {
		log.Error("initChannel channel %s, %v", s.tid, err)
		return err
	}
	err = channel.ExchangeDeclare(s.conf.ExchangeName, s.conf.ExchangeType, false,
		false, false, false, nil)
	if err != nil {
		log.Error("initChannel ExchangeDeclare %s, %v", s.tid, err)
		return
	}
	_, err = channel.QueueDeclare(
		s.conf.QueueName,
		true,
		false,
		false,
		false,
		s.delayArgs,
	)
	if err != nil {
		log.Error("initChannel QueueDeclare %s, %v", s.tid, err)
		return
	}
	err = channel.QueueBind(
		s.conf.QueueName,
		s.conf.QueueName,
		s.conf.ExchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Error("initChannel QueueBind %s, %v", s.tid, err)
		return
	}
	s.changeChannel(channel)
	s.isChannelReady.Store(true)
	go s.handleReInitChannel()
	log.Info("initChannel success! %s", s.tid)
	return
}

func (s *Client) changeChannel(channel *amqp.Channel) {
	s.channel = channel
	s.notifyChanClose = make(chan *amqp.Error)
	s.channel.NotifyClose(s.notifyChanClose)
}

func (s *Client) handleReInitChannel() {

	time.Sleep(time.Second)

	for range s.notifyChanClose {

		log.Error("handleReInitChannel notifyChanClose. Re-running initChannel... %s", s.tid)
		try := 0
		time.Sleep(reInitDelay * 2) //wait conn

	ReTry:
		try++
		if s.isClosed.Load() {
			log.Warn("handleReInitChannel isClosed true.%s , %v ", s.tid, ErrShutdown)
			return
		}

		if !s.isConnected.Load() {
			log.Warn("handleReInitChannel isConnected false wait. %s", s.tid)
			time.Sleep(reInitDelay * 2)
			goto ReTry
		}

		log.Warn("handleReInitChannel to init channel %s ,try %d", s.tid, try)

		err := s.initChannel()
		if err != nil {
			log.Error("handleReInitChannel failed to initialize channel. Retrying...%s ,try:%d, err:%v", s.tid, try, err)
			time.Sleep(tryIntervalDuration(try, s.conf.FailedRetryCount))
			goto ReTry
		}
		return
	}

}

func (s *Client) Pub(ctx context.Context, message []byte) error {
	lg := log.WithContext(ctx)
	if err := s.isReady(); err != nil {
		lg.Errorf("Pub %s, %v", s.tid, err)
		return err
	}
	if s.action != actionPub {
		lg.Errorf("Pub client not support, %s", s.tid)
		return ErrNotSupportOperation
	}
	/*	traceHeader, err := injectTraceHeader(ctx)
		if err != nil {
			lg.Errorf(ctx, "%s, %v", message, err)
		}*/

	return s.channel.Publish(
		s.conf.ExchangeName, // Exchange
		s.conf.QueueName,    // Routing key
		false,               // Mandatory
		false,               // Immediate
		amqp.Publishing{
			ContentType: msgContentType,
			//Headers:     traceHeader,
			Body: message,
		},
	)
}

// PubDelay  delay use second
func (s *Client) PubDelay(ctx context.Context, message []byte, delay time.Duration) error {
	lg := log.WithContext(ctx)
	if err := s.isReady(); err != nil {
		lg.Errorf( "PubDelay %s, %v", s.tid, err)
		return err
	}
	if s.action != actionDelay {
		lg.Errorf( "PubDelay client not support delay %s", s.tid)
		return ErrNotSupportOperation
	}
	/*	traceHeader, err := injectTraceHeader(ctx)
		if err != nil {
			lg.Errorf(ctx, "%s, %v", message, err)
		}*/

	return s.channel.Publish(
		s.conf.ExchangeName,
		s.conf.QueueName,
		false,
		false,
		amqp.Publishing{
			ContentType: msgContentType,
			Body:        message,
			Expiration:  strconv.FormatInt(delay.Milliseconds(), 10),
			//	Headers:     traceHeader,
		})
}

//Messages autoAck 默认false，需要手动ack
func (s *Client) Messages() (<-chan Message, error) {
	message := make(chan Message)

	go func() {

		for {

			if s.isClosed.Load() {
				close(message)
				log.Warn("[Messages] Consume ,cli isClosed, end %s", s.tid)
				return
			}

			err := s.isReady()
			if err != nil {
				log.Warn("[Messages] Consume err,try... %s, %v", s.tid, err)
				time.Sleep(reInitDelay * 3) //wait re connect and re channel
				continue
			}

			d, err := s.channel.Consume(
				s.conf.QueueName,
				"",
				false,
				false,
				false,
				false,
				nil,
			)
			if err != nil {
				log.Error("[Messages] Consume err, try reconsume... %s, %v", s.tid, err)
				time.Sleep(reInitDelay * 3)
				continue
			}

			log.Info("[Messages] Consume success! %s", s.tid)
			for msg := range d {
				message <- Message{msg}
			}

			log.Warn("[Messages] Consume Delivery closed, try reconsume... %s", s.tid)

		}
	}()

	return message, nil
}

func (s *Client) Close() error {

	if !s.isClosed.CAS(false, true) {
		log.Warn("Client Shutdown close %s, %v", s.tid, ErrShutdown)
		return ErrShutdown
	}
	err := s.channel.Close()
	if err != nil {
		log.Warn("Client channel close %s, %v", s.tid, err)
	}
	err = s.connection.Close()
	if err != nil {
		log.Warn("Client connection close %s, %v", s.tid, err)
	}

	return nil
}

func (s *Client) isReady() error {

	if !s.isConnected.Load() {
		return ErrNotConnected
	}
	if !s.isChannelReady.Load() {
		return ErrNotConnected
	}
	if s.isClosed.Load() {
		return ErrShutdown
	}

	return nil
}

/*
//Context 从消息header中析出trace注入到context中
func (m *Message) Context() (ctx context.Context) {
	ctx = context.Background()
	var err error
	defer func() {
		if err != nil {
			log.Warn("find trace err:%v, use new trace", err)
			ctx = trace.NewContext(ctx, trace.New(traceMqOpt))
		}
	}()
	v, ok := m.Headers[rmqTrace]
	if !ok {
		err = errors.New("un find trace in message header")
		return
	}
	vv, ok := v.(string)
	if !ok {
		err = errors.New("trace convert string err")
		return
	}
	headers := make(map[string]string)
	err = jsoniter.UnmarshalFromString(vv, &headers)
	if err != nil {
		return
	}
	t, err := trace.Extract(trace.MapFormat, headers)
	if err != nil {
		return
	}
	ctx = trace.NewContext(ctx, t)
	return
}

// ContextWithTimeout  get context with timeout
func (m *Message) ContextWithTimeout(timeout time.Duration) (ctx context.Context, cancel context.CancelFunc) {
	return context.WithTimeout(m.Context(), timeout)
}

//injectTraceHeader 从context析出trace, 注入到消息header
func injectTraceHeader(ctx context.Context) (resp amqp.Table, err error) {

	resp = make(amqp.Table)
	tr, ok := trace.FromContext(ctx)
	if !ok {
		err = errors.New("injectTraceHeader unFind trace from ctx")
		return
	}
	headers := make(map[string]string)
	err = trace.Inject(tr, trace.MapFormat, headers)
	if err != nil {
		return
	}
	hs, err := jsoniter.MarshalToString(headers)
	if err != nil {
		return
	}

	resp[rmqTrace] = hs
	return
}*/

func isDelayAction(action string) bool {
	return strings.Contains(action, actionDelayStr)
}
func isPubAction(action string) bool {
	return strings.Contains(action, actionPubStr)
}
func isSubAction(action string) bool {
	return strings.Contains(action, actionSubStr)
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}

func tryIntervalDuration(try int, max int) time.Duration {
	if try < max {
		return time.Duration(try) * time.Second
	}
	return time.Duration(max) * time.Second
}

func (s *Client) Conf() *Config {
	return s.conf
}
