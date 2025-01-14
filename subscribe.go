package corroclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
)

type subscribeOptions struct {
	backoff    time.Duration
	maxRetries int
	skipRows   bool
	bufferSize int
}

type SubOpt func(*subscribeOptions)

func WithRetryOptions(backoff time.Duration, maxRetries int) SubOpt {
	return func(o *subscribeOptions) {
		o.backoff = backoff
		o.maxRetries = maxRetries
	}
}

func WithSkipRows() SubOpt {
	return func(o *subscribeOptions) {
		o.skipRows = true
	}
}

func WithBufferSize(size int) SubOpt {
	return func(o *subscribeOptions) {
		o.bufferSize = size
	}
}

func (c *CorroClient) Subscribe(ctx context.Context, statement Statement, opts ...SubOpt) (*Subscription, error) {
	options := subscribeOptions{
		backoff:    1 * time.Second,
		maxRetries: 10,
		bufferSize: 5,
	}
	for _, opt := range opts {
		opt(&options)
	}

	resp, err := c.postSubscription(ctx, statement, false, 0)
	if err != nil {
		return nil, err
	}

	id := resp.Header.Get("Corro-Query-Id")
	hash := resp.Header.Get("Corro-Sub-Hash")

	subCtx, cancel := context.WithCancel(context.Background())

	sub := &Subscription{
		client:     c,
		id:         id,
		hash:       hash,
		body:       resp.Body,
		subCtx:     subCtx,
		cancel:     cancel,
		events:     make(chan Event, options.bufferSize),
		errored:    make(chan struct{}),
		maxRetries: options.maxRetries,
		backoff:    options.backoff,
	}

	go sub.run()

	return sub, nil
}

func (s *Subscription) readNext(reader *bufio.Reader) (Event, error) {
	raw, err := readNextRaw(reader)
	if err != nil {
		return nil, err
	}

	e, err := readEvent(raw)
	if err != nil {
		return nil, ErrUnrecoverableSub // We can't recover properly from this, so we close the subscription
	}

	switch e := e.(type) {
	case *Columns:
		s.columns = *e
	case *Row:
		e.columns = s.columns
	case *EOQ:
		s.seenEoq = true
		s.lastChangeId = e.ChangeId
	case *Change:
		if e.ChangeId != s.lastChangeId+1 {
			return nil, ErrMissedChange
		}
		s.lastChangeId = e.ChangeId
	}

	return e, nil
}

type Subscription struct {
	client *CorroClient
	id     string
	hash   string

	subCtx context.Context
	cancel context.CancelFunc

	body io.ReadCloser

	events  chan Event
	errored chan struct{}
	err     error

	lastChangeId uint64
	seenEoq      bool
	columns      []string

	maxRetries int
	retries    int
	backoff    time.Duration
}

func (s *Subscription) hasBeenClosed() bool {
	return s.subCtx.Err() != nil
}

func readNextRaw(reader *bufio.Reader) ([]byte, error) {
	eventData, _, err := reader.ReadLine()
	if err != nil {
		return nil, err
	}

	return eventData, nil
}

func readEvent(data []byte) (Event, error) {
	var e event
	err := json.Unmarshal(data, &e)
	if err != nil {
		return nil, err
	}

	if e.Columns != nil {
		return e.Columns, nil
	}

	if e.Row != nil {
		return readRow(e.Row)
	}

	if e.Change != nil {
		return readChange(e.Change)
	}

	if e.EOQ != nil {
		return e.EOQ, nil
	}

	if e.Error != nil {
		return &Error{err: errors.New(*e.Error)}, nil
	}

	return nil, ErrUnknownEvent
}

func (s *Subscription) run() {
	reader := bufio.NewReader(s.body)
	defer func() {
		if s.body != nil {
			s.body.Close()
		}
	}()
MAIN:
	for {
		e, err := s.readNext(reader)
		if err == nil {
			// When corrosion sends an error, it's fatal
			if e.Type() == EventTypeError {
				close(s.errored)
				s.err = e.(*Error).err
				return
			}
			s.events <- e
			continue
		}

		// If the subscription has been closed, we don't need to do anything
		if s.hasBeenClosed() {
			return
		}

		// If connection has been closed by corrosion, we need to recover
		if err == io.EOF || err == io.ErrClosedPipe {
			s.body.Close()
			for s.retries < s.maxRetries {
				if s.hasBeenClosed() {
					return
				}
				s.retries++
				err = s.recoverConn()
				if err == nil {
					s.retries = 0
					reader = bufio.NewReader(s.body)
					continue MAIN
				}
				if err == ErrUnrecoverableSub {
					s.err = err
					close(s.errored)
					return
				}

				time.Sleep(s.backoff)
			}
			s.err = ErrMaxRetryExceeded
			close(s.errored)
			return
		}

		// Unknown error, close the subscription anyway
		s.err = err
		close(s.errored)
	}
}

func (s *Subscription) Next() (Event, error) {
	select {
	case e := <-s.events:
		return e, nil
	case <-s.errored:
		return nil, s.err
	case <-s.subCtx.Done():
		return nil, ErrSubscriptionClosed
	}
}

func (s *Subscription) Close() {
	s.cancel()
}

func (s *Subscription) recoverConn() error {
	if !s.seenEoq {
		return ErrUnrecoverableSub
	}

	resp, err := s.client.getSub(s.subCtx, s.id, true, s.lastChangeId)
	if err != nil {
		if err == errNotFound {
			return ErrUnrecoverableSub
		}
		return err
	}

	s.body = resp.Body

	return nil
}
