package queue

import (
	"context"
	"testing"
	"time"

	"github.com/mongodb/amboy"
	"github.com/mongodb/amboy/job"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func localQueueConstructor(ctx context.Context) (amboy.Queue, error) {
	q := NewLocalUnordered(1)
	if err := q.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}
	return q, nil
}

func remoteQueueConstructor(ctx context.Context, collection string) (amboy.Queue, error) {
	q := NewRemoteUnordered(1)
	d := NewMongoDriver(collection, MongoDBOptions{})
	if err := q.SetDriver(d); err != nil {
		return nil, errors.Wrap(err, "problem setting driver")
	}
	if err := q.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "problem starting queue")
	}
	return q, nil
}

func TestQueueGroupConstructor(t *testing.T) {
	for _, test := range []struct {
		name              string
		valid             bool
		localConstructor  amboy.LocalQueueConstructor
		remoteConstructor amboy.RemoteQueueConstructor
		ttl               time.Duration
	}{
		{
			name:              "NilConstructorNegativeTime",
			localConstructor:  nil,
			remoteConstructor: nil,
			valid:             false,
			ttl:               -time.Minute,
		},
		{
			name:              "NilConstructorZeroTime",
			localConstructor:  nil,
			remoteConstructor: nil,
			valid:             false,
			ttl:               0,
		},
		{
			name:              "NilConstructorPositiveTime",
			localConstructor:  nil,
			remoteConstructor: nil,
			valid:             false,
			ttl:               time.Minute,
		},
		{
			name:              "ConstructorNegativeTime",
			localConstructor:  localQueueConstructor,
			remoteConstructor: remoteQueueConstructor,
			valid:             false,
			ttl:               -time.Minute,
		},
		{
			name:              "ConstructorZeroTime",
			localConstructor:  localQueueConstructor,
			remoteConstructor: remoteQueueConstructor,
			valid:             true,
			ttl:               0,
		},
		{
			name:              "ConstructorPositiveTime",
			localConstructor:  localQueueConstructor,
			remoteConstructor: remoteQueueConstructor,
			valid:             true,
			ttl:               time.Minute,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Run("local", func(t *testing.T) {
				localOpts := LocalQueueGroupOptions{
					Constructor: test.localConstructor,
					TTL:         test.ttl,
				}
				g, err := NewLocalQueueGroup(context.Background(), localOpts)
				if test.valid {
					assert.NotNil(t, g)
					assert.NoError(t, err)
				} else {
					assert.Nil(t, g)
					assert.Error(t, err)
				}
			})
			for _, remoteTest := range []struct {
				name   string
				client *mongo.Client
				db     string
				prefix string
				uri    string
				valid  bool
			}{
				{
					name:   "AllFieldsSet",
					db:     "db",
					prefix: "prefix",
					uri:    "uri",
					valid:  true,
				},
				{
					name:   "DBMissing",
					prefix: "prefix",
					uri:    "uri",
					valid:  false,
				},
				{
					name:  "PrefixMissing",
					db:    "db",
					uri:   "uri",
					valid: false,
				},
				{
					name:   "URIMissing",
					db:     "db",
					prefix: "prefix",
					valid:  false,
				},
			} {

				t.Run(remoteTest.name, func(t *testing.T) {
					client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					require.NoError(t, client.Connect(ctx))
					remoteOpts := RemoteQueueGroupOptions{
						Client:      client,
						DB:          remoteTest.db,
						Prefix:      remoteTest.prefix,
						Constructor: test.remoteConstructor,
						TTL:         test.ttl,
						URI:         remoteTest.uri,
					}
					g, err := NewRemoteQueueGroup(context.Background(), remoteOpts)
					if test.valid && remoteTest.valid {
						assert.NotNil(t, g)
						assert.NoError(t, err)
					} else {
						assert.Nil(t, g)
						assert.Error(t, err)
					}
				})
			}
		})
	}
}

func TestQueueGroupOperations(t *testing.T) {
	t.Run("Get", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		g, err := NewLocalQueueGroup(context.Background(), LocalQueueGroupOptions{Constructor: localQueueConstructor})
		assert.NoError(t, err)
		assert.NotNil(t, g)

		q1, err := g.Get(ctx, "one")
		assert.NoError(t, err)
		assert.NotNil(t, q1)

		q2, err := g.Get(ctx, "two")
		assert.NoError(t, err)
		assert.NotNil(t, q2)

		j1 := job.NewShellJob("true", "")
		j2 := job.NewShellJob("true", "")
		j3 := job.NewShellJob("true", "")

		// Add j1 to q1. Add j2 and j3 to q2.
		assert.NoError(t, q1.Put(j1))
		assert.NoError(t, q2.Put(j2))
		assert.NoError(t, q2.Put(j3))

		amboy.Wait(q1)
		amboy.Wait(q2)

		resultsQ1 := []amboy.Job{}
		for result := range q1.Results(ctx) {
			resultsQ1 = append(resultsQ1, result)
		}
		resultsQ2 := []amboy.Job{}
		for result := range q2.Results(ctx) {
			resultsQ2 = append(resultsQ2, result)
		}

		assert.Len(t, resultsQ1, 1)
		assert.Len(t, resultsQ2, 2)

		// Try getting the queues again
		q1, err = g.Get(ctx, "one")
		assert.NoError(t, err)
		assert.NotNil(t, q1)

		q2, err = g.Get(ctx, "two")
		assert.NoError(t, err)
		assert.NotNil(t, q2)

		// The queues should be the same, i.e., contain the jobs we expect
		resultsQ1 = []amboy.Job{}
		for result := range q1.Results(ctx) {
			resultsQ1 = append(resultsQ1, result)
		}
		resultsQ2 = []amboy.Job{}
		for result := range q2.Results(ctx) {
			resultsQ2 = append(resultsQ2, result)
		}
	})

	t.Run("Put", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		g, err := NewLocalQueueGroup(context.Background(), LocalQueueGroupOptions{Constructor: localQueueConstructor})
		assert.NoError(t, err)
		assert.NotNil(t, g)

		q1, err := g.Get(ctx, "one")
		assert.NoError(t, err)
		assert.NotNil(t, q1)

		q2, err := localQueueConstructor(ctx)
		assert.NoError(t, err)
		assert.Error(t, g.Put("one", q2), "cannot add queue to existing index")

		q3, err := localQueueConstructor(ctx)
		assert.NoError(t, err)
		assert.NoError(t, g.Put("three", q3))

		q4, err := localQueueConstructor(ctx)
		assert.NoError(t, err)
		assert.NoError(t, g.Put("four", q4))

		j1 := job.NewShellJob("true", "")
		j2 := job.NewShellJob("true", "")
		j3 := job.NewShellJob("true", "")

		// Add j1 to q3. Add j2 and j3 to q4.
		assert.NoError(t, q3.Put(j1))
		assert.NoError(t, q4.Put(j2))
		assert.NoError(t, q4.Put(j3))

		amboy.Wait(q3)
		amboy.Wait(q4)

		resultsQ3 := []amboy.Job{}
		for result := range q3.Results(ctx) {
			resultsQ3 = append(resultsQ3, result)
		}
		resultsQ4 := []amboy.Job{}
		for result := range q4.Results(ctx) {
			resultsQ4 = append(resultsQ4, result)
		}

		assert.Len(t, resultsQ3, 1)
		assert.Len(t, resultsQ4, 2)

		// Try getting the queues again
		q3, err = g.Get(ctx, "one")
		assert.NoError(t, err)
		assert.NotNil(t, q3)

		q4, err = g.Get(ctx, "two")
		assert.NoError(t, err)
		assert.NotNil(t, q4)

		// The queues should be the same, i.e., contain the jobs we expect
		resultsQ3 = []amboy.Job{}
		for result := range q3.Results(ctx) {
			resultsQ3 = append(resultsQ3, result)
		}
		resultsQ4 = []amboy.Job{}
		for result := range q4.Results(ctx) {
			resultsQ4 = append(resultsQ4, result)
		}
	})

	t.Run("Prune", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		g, err := NewLocalQueueGroup(context.Background(), LocalQueueGroupOptions{Constructor: localQueueConstructor})
		assert.NoError(t, err)
		assert.NotNil(t, g)

		q1, err := g.Get(ctx, "one")
		assert.NoError(t, err)
		assert.NotNil(t, q1)

		q2, err := g.Get(ctx, "two")
		assert.NoError(t, err)
		assert.NotNil(t, q2)

		j1 := job.NewShellJob("true", "")
		j2 := job.NewShellJob("true", "")
		j3 := job.NewShellJob("true", "")

		// Add j1 to q1. Add j2 and j3 to q2.
		assert.NoError(t, q1.Put(j1))
		assert.NoError(t, q2.Put(j2))
		assert.NoError(t, q2.Put(j3))

		amboy.Wait(q1)
		amboy.Wait(q2)

		g.Prune()

		// Try getting the queues again
		q1, err = g.Get(ctx, "one")
		assert.NoError(t, err)
		assert.NotNil(t, q1)

		q2, err = g.Get(ctx, "two")
		assert.NoError(t, err)
		assert.NotNil(t, q2)

		// Queues should be empty, because they're new
		stats1 := q1.Stats()
		assert.Zero(t, stats1.Running)
		assert.Zero(t, stats1.Completed)
		assert.Zero(t, stats1.Pending)
		assert.Zero(t, stats1.Blocked)
		assert.Zero(t, stats1.Total)

		stats2 := q2.Stats()
		assert.Zero(t, stats2.Running)
		assert.Zero(t, stats2.Completed)
		assert.Zero(t, stats2.Pending)
		assert.Zero(t, stats2.Blocked)
		assert.Zero(t, stats2.Total)
	})

	t.Run("Close", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		g, err := NewLocalQueueGroup(context.Background(), LocalQueueGroupOptions{Constructor: localQueueConstructor})
		assert.NoError(t, err)
		assert.NotNil(t, g)

		q1, err := g.Get(ctx, "one")
		assert.NoError(t, err)
		assert.NotNil(t, q1)

		q2, err := g.Get(ctx, "two")
		assert.NoError(t, err)
		assert.NotNil(t, q2)

		j1 := job.NewShellJob("true", "")
		j2 := job.NewShellJob("true", "")
		j3 := job.NewShellJob("true", "")

		// Add j1 to q1. Add j2 and j3 to q2.
		assert.NoError(t, q1.Put(j1))
		assert.NoError(t, q2.Put(j2))
		assert.NoError(t, q2.Put(j3))

		amboy.Wait(q1)
		amboy.Wait(q2)

		g.Close(ctx)
	})
}

// func TestGetExistingCollections(t *testing.T) {
// 	t.Run("tmp", func(t *testing.T) {
// 		ctx, cancel := context.WithCancel(context.Background())
// 		defer cancel()

// 		client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://localhost:27017"))
// 		require.NoError(t, client.Connect(ctx))
// 		collection := client.Database("foo").Collection("my_coll")

// 		opts := RemoteQueueGroupOptions{
// 			Client:      client,
// 			DB:          "amboy_test",
// 			Prefix:      "prefix_",
// 			Constructor: nil,
// 			TTL:         time.Minute,
// 			// URI:         "mongodb://localhost:27017",
// 		}
// 		require.NoError(t, err)
// 		colls, err := getExistingCollections(ctx, opts)
// 		require.NoError(t, err)
// 		require.Len(t, colls, 0)
// 	})
// }
