package subpub

import (
	"context"
	"errors"
	"log"
	"sync"
)

type MessageHandler func(msg interface{})

type Subscription interface {
	Unsubscribe()
}

type SubPub interface {
	Subscribe(subject string, cb MessageHandler) (Subscription, error)

	Publish(subject string, msg interface{}) error

	Close(ctx context.Context) error
}

type subscriber struct {
	handler  MessageHandler
	queue    chan interface{}
	stopOnce sync.Once
	stopCh   chan struct{}
	wg       *sync.WaitGroup
}

type subscription struct {
	sub     *subscriber
	subpub  *subPubImpl
	subject string
}

type subPubImpl struct {
	mu          sync.RWMutex
	subscribers map[string][]*subscriber
	closed      bool
	wg          sync.WaitGroup
}

func NewSubPub() SubPub {
	return &subPubImpl{
		subscribers: make(map[string][]*subscriber),
	}
}

func (sp *subPubImpl) Subscribe(subject string, cb MessageHandler) (Subscription, error) {
	sp.mu.Lock()
	defer sp.mu.Unlock()
	if sp.closed {
		return nil, errors.New("subpub is closed")
	}

	sub := &subscriber{
		handler: cb,
		queue:   make(chan interface{}, 256),
		stopCh:  make(chan struct{}),
		wg:      &sp.wg,
	}

	sp.subscribers[subject] = append(sp.subscribers[subject], sub)
	sp.wg.Add(1)
	go sub.run()

	return &subscription{sub: sub, subpub: sp, subject: subject}, nil
}

func (s *subscriber) run() {
	defer func() {
		close(s.queue)
		s.wg.Done()
	}()

	for {
		select {
		case msg, ok := <-s.queue:
			if !ok {
				return
			}
			s.handler(msg)
		case <-s.stopCh:
			for {
				select {
				case msg, ok := <-s.queue:
					if !ok {
						return
					}
					s.handler(msg)
				default:
					return
				}
			}
		}
	}
}

func (sp *subPubImpl) Publish(subject string, msg interface{}) error {
	sp.mu.RLock()

	if sp.closed {
		sp.mu.RUnlock()
		return errors.New("subpub is closed")
	}

	subs, ok := sp.subscribers[subject]
	if !ok {
		sp.mu.RUnlock()
		return nil
	}

	defer sp.mu.RUnlock()

	for _, sub := range subs {
		select {
		case sub.queue <- msg:
		default:
			log.Printf("subpub: message dropped for subscriber to subject '%s' (queue full)", subject)
		}
	}
	return nil
}

func (s *subscription) Unsubscribe() {
	s.subpub.mu.Lock()

	subs, ok := s.subpub.subscribers[s.subject]
	if !ok {
		s.subpub.mu.Unlock()
		return
	}

	foundIdx := -1
	for i, sub := range subs {
		if sub == s.sub {
			foundIdx = i
			break
		}
	}

	if foundIdx != -1 {
		s.subpub.subscribers[s.subject] = append(subs[:foundIdx], subs[foundIdx+1:]...)
		if len(s.subpub.subscribers[s.subject]) == 0 {
			delete(s.subpub.subscribers, s.subject)
		}
	}
	s.subpub.mu.Unlock()

	if foundIdx != -1 {
		s.sub.stopOnce.Do(func() {
			close(s.sub.stopCh)
		})
	}
}

func (sp *subPubImpl) Close(ctx context.Context) error {
	sp.mu.Lock()
	if sp.closed {
		sp.mu.Unlock()
		return nil
	}
	sp.closed = true

	for _, subs := range sp.subscribers {
		for _, sub := range subs {
			sub.stopOnce.Do(func() {
				close(sub.stopCh)
			})
		}
	}
	sp.mu.Unlock()
	done := make(chan struct{})
	go func() {
		sp.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}
