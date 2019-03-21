package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/grip/recovery"
	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// localQueueGroup is a group of in-memory queues.
type localQueueGroup struct {
	mu          sync.RWMutex
	canceler    context.CancelFunc
	queues      map[string]amboy.Queue
	constructor amboy.LocalQueueConstructor
	ttlMap      map[string]time.Time
	ttl         time.Duration
}

type LocalQueueGroupOptions struct {
	Constructor amboy.LocalQueueConstructor
	TTL         time.Duration
}

// NewLocalQueueGroup constructs a new local queue group. If ttl is 0, the queues will not be
// TTLed except when the client explicitly calls Prune.
func NewLocalQueueGroup(ctx context.Context, opts LocalQueueGroupOptions) (amboy.QueueGroup, error) {
	if opts.Constructor == nil {
		return nil, errors.New("must pass a constructor")
	}
	if opts.TTL < 0 {
		return nil, errors.New("ttl must be greater than or equal to 0")
	}
	if opts.TTL > 0 && opts.TTL < time.Second {
		return nil, errors.New("ttl cannot be less than 1 second, unless it is 0")
	}
	ctx, cancel := context.WithCancel(ctx)
	g := &localQueueGroup{
		canceler:    cancel,
		queues:      map[string]amboy.Queue{},
		constructor: opts.Constructor,
		ttlMap:      map[string]time.Time{},
		ttl:         opts.TTL,
	}
	if opts.TTL > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in local queue group ticker")
			ticker := time.NewTicker(opts.TTL)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.Prune()
				}
			}
		}()
	}
	return g, nil
}

// Get a queue with the given index. Get sets the last accessed time to now. Note that this means
// that the caller must add a job to the queue within the TTL, or else it may have attempted to add
// a job to a closed queue.
func (g *localQueueGroup) Get(ctx context.Context, id string) (amboy.Queue, error) {
	g.mu.RLock()
	if queue, ok := g.queues[id]; ok {
		g.ttlMap[id] = time.Now()
		g.mu.RUnlock()
		return queue, nil
	}
	g.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	// Check again in case the map was modified after we released the read lock.
	if queue, ok := g.queues[id]; ok {
		g.ttlMap[id] = time.Now()
		return queue, nil
	}

	queue, err := g.constructor(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "problem constructing queue")
	}
	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return queue, nil
}

// Put a queue at the given index.
func (g *localQueueGroup) Put(id string, queue amboy.Queue) error {
	g.mu.RLock()
	if _, ok := g.queues[id]; ok {
		g.mu.RUnlock()
		return errors.New("a queue already exists at this index")
	}
	g.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	// Check again in case the map was modified after we released the read lock.
	if _, ok := g.queues[id]; ok {
		return errors.New("a queue already exists at this index")
	}

	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return nil
}

// Prune old queues.
func (g *localQueueGroup) Prune() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	queues := make([]amboy.Queue, 0, len(g.ttlMap))
	i := 0
	for queueID, t := range g.ttlMap {
		if time.Since(t) > g.ttl {
			if q, ok := g.queues[queueID]; ok {
				delete(g.queues, queueID)
				queues = append(queues, q)
			}
			delete(g.ttlMap, queueID)
		}
		i++
	}
	wg := &sync.WaitGroup{}
	for _, queue := range queues {
		wg.Add(1)
		go func(queue amboy.Queue) {
			queue.Runner().Close()
			wg.Done()
		}(queue)
	}
	wg.Wait()
	return nil
}

// Close the queues.
func (g *localQueueGroup) Close(ctx context.Context) {
	g.canceler()
	waitCh := make(chan struct{})
	wg := &sync.WaitGroup{}
	go func() {
		for _, queue := range g.queues {
			wg.Add(1)
			go func(queue amboy.Queue) {
				defer recovery.LogStackTraceAndContinue("panic in local queue group closer")
				defer wg.Done()
				queue.Runner().Close()
			}(queue)
		}
		wg.Wait()
		close(waitCh)
	}()
	select {
	case <-waitCh:
		return
	case <-ctx.Done():
		return
	}
}

// remoteQueueGroup is a group of in-memory queues.
type remoteQueueGroup struct {
	mu          sync.RWMutex
	canceler    context.CancelFunc
	client      *mongo.Client
	queues      map[string]amboy.Queue
	constructor amboy.RemoteQueueConstructor
	ttlMap      map[string]time.Time
	ttl         time.Duration
}

type RemoteQueueGroupOptions struct {
	Client      *mongo.Client
	DB          string
	Prefix      string
	Constructor amboy.RemoteQueueConstructor
	TTL         time.Duration
	URI         string
}

type listCollectionsOutput struct {
	Name string `bson:"name"`
}

// NewRemoteQueueGroup constructs a new remote queue group. If ttl is 0, the queues will not be
// TTLed except when the client explicitly calls Prune.
func NewRemoteQueueGroup(ctx context.Context, opts RemoteQueueGroupOptions) (amboy.QueueGroup, error) {
	if opts.Client == nil {
		return nil, errors.New("must pass a client")
	}
	if opts.Constructor == nil {
		return nil, errors.New("must pass a constructor")
	}
	if opts.TTL < 0 {
		return nil, errors.New("ttl must be greater than or equal to 0")
	}
	if opts.TTL > 0 && opts.TTL < time.Second {
		return nil, errors.New("ttl cannot be less than 1 second, unless it is 0")
	}
	if opts.DB == "" {
		return nil, errors.New("db must be set")
	}
	if opts.Prefix == "" {
		return nil, errors.New("prefix must be set")
	}
	if opts.URI == "" {
		return nil, errors.New("uri must be set")
	}

	ctx, cancel := context.WithCancel(ctx)

	colls, err := getExistingCollections(ctx, opts)
	if err != nil {
		return nil, errors.Wrap(err, "problem getting existing collections")
	}
	for _, coll := range colls {
		fmt.Print(coll)
	}

	g := &remoteQueueGroup{
		canceler:    cancel,
		client:      opts.Client,
		queues:      map[string]amboy.Queue{},
		constructor: opts.Constructor,
		ttlMap:      map[string]time.Time{},
		ttl:         opts.TTL,
	}
	if opts.TTL > 0 {
		go func() {
			defer recovery.LogStackTraceAndContinue("panic in remote queue group ticker")
			ticker := time.NewTicker(opts.TTL)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					g.Prune()
				}
			}
		}()
	}
	return g, nil
}

func getExistingCollections(ctx context.Context, opts RemoteQueueGroupOptions) ([]string, error) {
	c, err := opts.Client.Database(opts.DB).ListCollections(ctx, bson.M{"name": bson.M{"$regex": fmt.Sprintf("^%s.*", opts.Prefix)}})
	if err != nil {
		return nil, errors.Wrap(err, "problem calling listCollections")
	}
	defer c.Close(ctx)
	var collections []listCollectionsOutput
	for c.Next(ctx) {
		elem := &listCollectionsOutput{}
		if err := c.Decode(elem); err != nil {
			return nil, errors.Wrap(err, "problem parsing listCollections output")
		}
		collections = append(collections, *elem)
	}
	collNames := []string{}
	for _, coll := range collections {
		collNames = append(collNames, coll.Name)
	}
	return collNames, nil
}

// Get a queue with the given index. Get sets the last accessed time to now. Note that this means
// that the caller must add a job to the queue within the TTL, or else it may have attempted to add
// a job to a closed queue.
func (g *remoteQueueGroup) Get(ctx context.Context, id string) (amboy.Queue, error) {
	g.mu.RLock()
	if queue, ok := g.queues[id]; ok {
		g.ttlMap[id] = time.Now()
		g.mu.RUnlock()
		return queue, nil
	}
	g.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	// Check again in case the map was modified after we released the read lock.
	if queue, ok := g.queues[id]; ok {
		g.ttlMap[id] = time.Now()
		return queue, nil
	}

	queue, err := g.constructor(ctx, "")
	if err != nil {
		return nil, errors.Wrap(err, "problem constructing queue")
	}
	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return queue, nil
}

// Put a queue at the given index.
func (g *remoteQueueGroup) Put(id string, queue amboy.Queue) error {
	g.mu.RLock()
	if _, ok := g.queues[id]; ok {
		g.mu.RUnlock()
		return errors.New("a queue already exists at this index")
	}
	g.mu.RUnlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	// Check again in case the map was modified after we released the read lock.
	if _, ok := g.queues[id]; ok {
		return errors.New("a queue already exists at this index")
	}

	g.queues[id] = queue
	g.ttlMap[id] = time.Now()
	return nil
}

// Prune old queues.
func (g *remoteQueueGroup) Prune() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	queues := make([]amboy.Queue, 0, len(g.ttlMap))
	i := 0
	for queueID, t := range g.ttlMap {
		if time.Since(t) > g.ttl {
			if q, ok := g.queues[queueID]; ok {
				delete(g.queues, queueID)
				queues = append(queues, q)
			}
			delete(g.ttlMap, queueID)
		}
		i++
	}
	wg := &sync.WaitGroup{}
	for _, queue := range queues {
		wg.Add(1)
		go func(queue amboy.Queue) {
			queue.Runner().Close()
			wg.Done()
		}(queue)
	}
	wg.Wait()
	return nil
}

// Close the queues.
func (g *remoteQueueGroup) Close(ctx context.Context) {
	g.canceler()
	waitCh := make(chan struct{})
	wg := &sync.WaitGroup{}
	go func() {
		for _, queue := range g.queues {
			wg.Add(1)
			go func(queue amboy.Queue) {
				defer recovery.LogStackTraceAndContinue("panic in remote queue group closer")
				defer wg.Done()
				queue.Runner().Close()
			}(queue)
		}
		wg.Wait()
		close(waitCh)
	}()
	select {
	case <-waitCh:
		return
	case <-ctx.Done():
		return
	}
}
