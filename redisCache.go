package cache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/blutspende/bloodlab-common/pagination"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

type RedisCache interface {
	// Initialization
	Init(config RedisCacheConfig, refreshFillerFunc func(ctx context.Context) error, refreshInitFunc func(ctx context.Context) error)
	// Refreshing and validity
	IsValid(ctx context.Context) bool
	SetToInvalid(ctx context.Context)
	SetToValid(ctx context.Context)
	RefreshCacheAsync(ctx context.Context, forceUpdate bool)
	// CRUD
	Store(ctx context.Context, key string, content interface{}) error
	StoreWithExpiration(ctx context.Context, key string, content interface{}, expirationTime *time.Duration) error
	Read(ctx context.Context, key string, modelPtr interface{}) error
	ReadWithExpiration(ctx context.Context, key string, modelPtr interface{}, expirationTime *time.Duration) error
	ReadGroup(ctx context.Context, keys []string, modelArrayPtr interface{}) error
	Delete(ctx context.Context, key string) error
	// Set handling
	AddItemToSet(ctx context.Context, key string, item string) error
	IsItemInSet(ctx context.Context, key string, item string) (bool, error)
	GetItemsInSetAsMap(ctx context.Context, key string) (map[string]struct{}, error)
	DeleteItemFromSet(ctx context.Context, key string, item string) error
	// Flag handling
	SetFlag(ctx context.Context, key string) error
	SetFlagWithExpiration(ctx context.Context, key string, expirationTime *time.Duration) error
	GetFlag(ctx context.Context, key string) (bool, error)
	DeleteFlag(ctx context.Context, key string) error
	// Index handling
	CreateIndex(ctx context.Context, index string, options *redis.FTCreateOptions, fieldSchemas []*redis.FieldSchema) (string, error)
	SearchInIndex(ctx context.Context, indexName string, queryString string, options *redis.FTSearchOptions, modelArrayPtr interface{}) (totalCount int, err error)
	DeleteIndex(ctx context.Context, index string, deleteDocuments bool) error
	// Key handling
	KeyForAll() string
	KeyForOne(id uuid.UUID) string
	KeyForPage(page pagination.PaginatedQuery) string
	KeyForCustomPage(page pagination.PaginatedQuery, customKey string) string
	KeyForCustom(customKey string) string
	KeyForValuedCustom(name string, values ...string) string
	KeyForNotFound() string
}

type redisCache struct {
	redisClient          *redis.Client
	name                 string
	refreshMutex         *sync.Mutex
	rnd                  rand.Rand
	cacheValid           bool
	forceUpdateRequested bool
	config               *RedisCacheConfig
	refreshFillerFunc    func(ctx context.Context) error
	refreshInitFunc      func(ctx context.Context) error
}

func NewRedisCache(redisClient *redis.Client, appName, cacheName string) RedisCache {
	return &redisCache{
		redisClient:          redisClient,
		name:                 fmt.Sprintf("%s:%s", NameToKey(appName), NameToKey(cacheName)),
		refreshMutex:         &sync.Mutex{},
		rnd:                  *rand.New(rand.NewSource(time.Now().UnixNano())),
		cacheValid:           false,
		forceUpdateRequested: false,
		config:               nil,
		refreshFillerFunc:    nil,
		refreshInitFunc:      nil,
	}
}

// Constants and errors

const (
	MsgItemNotFound = "item not found in cache"
)

var (
	ErrCacheInvalid          = errors.New("cache is invalid")
	ErrItemNotFound          = errors.New(MsgItemNotFound)
	ErrNoSuchIndexFound      = errors.New("no such index found in cache")
	ErrConfigNotSet          = errors.New("configuration not initialized")
	ErrExpirationNotSet      = errors.New("no default expiration set and no specific expiration provided")
	ErrMutexExpirationNotSet = errors.New("no mutex expiration set for multiserver mode")
	ErrNoClientSet           = errors.New("no redis client set in cache")
	ErrCachingDisabled       = errors.New("redis caching is disabled")
)

var (
	cacheValidFlagKey = "CACHE_VALID"
	mutexLockFlagKey  = "MUTEX_LOCK"
)

// Config

type RedisCacheConfig struct {
	RefreshRetryAttempts     int
	RefreshRetryWaitStartMs  int
	RefreshRetryWaitExponent int
	DefaultExpiration        *time.Duration
	MultiserverMode          bool
	MutexExpiration          *time.Duration
	IsDisabled               bool
}

// Initialization

func (c *redisCache) Init(config RedisCacheConfig, refreshFillerFunc func(ctx context.Context) error, refreshInitFunc func(ctx context.Context) error) {
	c.config = &config
	c.refreshFillerFunc = refreshFillerFunc
	c.refreshInitFunc = refreshInitFunc
	if c.config.IsDisabled {
		log.Warn().Msg(c.fmtMsg("redis cache is disabled"))
	}
}

// Refreshing and validity

func (c *redisCache) IsValid(ctx context.Context) bool {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return false
	}
	if c.config.IsDisabled {
		return false
	}
	if c.config != nil && c.config.MultiserverMode {
		valid, err := c.GetFlag(ctx, c.keyForSystem(cacheValidFlagKey))
		if err != nil {
			log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("checking validity failed"))
			return false
		}
		return valid
	}
	return c.cacheValid
}
func (c *redisCache) SetToInvalid(ctx context.Context) {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return
	}
	if c.config.IsDisabled {
		return
	}
	if c.config != nil && c.config.MultiserverMode {
		err := c.DeleteFlag(ctx, c.keyForSystem(cacheValidFlagKey))
		if err != nil {
			log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("setting to invalid failed"))
		}
	} else {
		c.cacheValid = false
	}
}
func (c *redisCache) SetToValid(ctx context.Context) {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return
	}
	if c.config.IsDisabled {
		return
	}
	if c.config != nil && c.config.MultiserverMode {
		err := c.SetFlag(ctx, c.keyForSystem(cacheValidFlagKey))
		if err != nil {
			log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("setting to valid failed"))
		}
	} else {
		c.cacheValid = true
	}
}

func (c *redisCache) mutexTryLock(ctx context.Context) bool {
	if c.config != nil && c.config.MultiserverMode {
		locked, err := c.GetFlag(ctx, c.keyForSystem(mutexLockFlagKey))
		if err != nil {
			log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("getting mutex flag failed"))
			return false
		}
		if locked {
			return false
		}
		if c.config.MutexExpiration == nil {
			log.Error().Ctx(ctx).Err(c.fmtErr(ErrMutexExpirationNotSet)).Send()
			return false
		}
		err = c.SetFlagWithExpiration(ctx, c.keyForSystem(mutexLockFlagKey), c.config.MutexExpiration)
		if err != nil {
			log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("setting mutex flag failed"))
			return false
		}
		return true
	}
	return c.refreshMutex.TryLock()
}
func (c *redisCache) mutexUnlock(ctx context.Context) {
	if c.config != nil && c.config.MultiserverMode {
		err := c.DeleteFlag(ctx, c.keyForSystem(mutexLockFlagKey))
		if err != nil {
			log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("unlocking mutex flag failed"))
		}
	} else {
		c.refreshMutex.Unlock()
	}
}

func (c *redisCache) RefreshCacheAsync(ctx context.Context, forceUpdate bool) {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return
	}
	if c.config.IsDisabled {
		return
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return
	}
	if !c.mutexTryLock(ctx) {
		if forceUpdate {
			c.forceUpdateRequested = true
			log.Debug().Ctx(ctx).Msg(c.fmtMsg("refresh already running, but force update requested"))
		} else {
			log.Debug().Ctx(ctx).Msg(c.fmtMsg("refresh already running, skipping new request"))
		}
		return
	}
	c.SetToInvalid(ctx)
	go func() {
		defer func() {
			c.mutexUnlock(ctx)
			if c.forceUpdateRequested {
				log.Debug().Ctx(ctx).Msg(c.fmtMsg("processing forced re-refresh request"))
				c.forceUpdateRequested = false
				go c.RefreshCacheAsync(ctx, false)
			}
		}()
		err := c.retry(c.config.RefreshRetryAttempts, (time.Duration)(c.config.RefreshRetryWaitStartMs)*time.Millisecond, (float64)(c.config.RefreshRetryWaitExponent), func() error {
			err := c.clearCache(ctx)
			if err != nil {
				log.Warn().Ctx(ctx).Err(err).Msg(c.fmtMsg("refresh clear cache failed"))
				return err
			}
			if c.refreshInitFunc != nil {
				err = c.refreshInitFunc(ctx)
				if err != nil {
					log.Warn().Ctx(ctx).Err(err).Msg(c.fmtMsg("refresh init function failed"))
					return err
				}
			}
			if c.refreshFillerFunc != nil {
				err = c.refreshFillerFunc(ctx)
				if err != nil {
					log.Warn().Ctx(ctx).Err(err).Msg(c.fmtMsg("refresh refill cache function failed"))
					return err
				}
			} else {
				log.Error().Ctx(ctx).Msg(c.fmtMsg("refresh called with no filler function provided"))
			}
			c.SetToValid(ctx)
			return nil
		})
		if err != nil {
			log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("refresh failed"))
		} else {
			log.Trace().Ctx(ctx).Msg(c.fmtMsg("refresh succeeded"))
		}
	}()
	return
}

// retry executes fn with progressive backoff
func (c *redisCache) retry(attempts int, sleep time.Duration, exponent float64, fn func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		backoff := time.Duration(math.Pow(exponent, float64(i))) * sleep
		jitter := time.Duration(c.rnd.Int63n(int64(sleep)))
		wait := backoff + jitter
		log.Warn().Msg(c.fmtMsg(fmt.Sprintf("attempt %d failed: %v, retrying in %v...\n", i+1, err, wait)))
		time.Sleep(wait)
	}
	return c.fmtErr(fmt.Errorf("after %d attempts, last error: %w", attempts, err))
}

// clearCache removes all keys with the cache's prefix
func (c *redisCache) clearCache(ctx context.Context) error {
	prefix := fmt.Sprintf("%s:*", c.name)
	var cursor uint64
	for {
		var keys []string
		var err error
		keys, cursor, err = c.redisClient.Scan(ctx, cursor, prefix, 50).Result()
		if err != nil {
			log.Error().Ctx(ctx).Interface("prefix", prefix).Msg(c.fmtMsg("deleting all existing keys by prefix failed"))
			return err
		}
		c.redisClient.Del(ctx, keys...)
		if cursor == 0 {
			break
		}
	}
	return nil
}

// CRUD

func (c *redisCache) Store(ctx context.Context, key string, content interface{}) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	return c.redisClient.JSONSet(ctx, key, "$", content).Err()
}
func (c *redisCache) StoreWithExpiration(ctx context.Context, key string, content interface{}, expirationTime *time.Duration) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	expiration := c.config.DefaultExpiration
	if expirationTime != nil {
		expiration = expirationTime
	}
	if expiration == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrExpirationNotSet)).Send()
		return ErrExpirationNotSet
	}
	pipeline := c.redisClient.Pipeline()
	pipeline.JSONSet(ctx, key, "$", content)
	pipeline.Expire(ctx, key, *expiration)
	_, err := pipeline.Exec(ctx)
	return err
}
func (c *redisCache) Read(ctx context.Context, key string, modelPtr interface{}) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	return c.read(ctx, key, modelPtr, nil)
}
func (c *redisCache) ReadWithExpiration(ctx context.Context, key string, modelPtr interface{}, expirationTime *time.Duration) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	expiration := c.config.DefaultExpiration
	if expirationTime != nil {
		expiration = expirationTime
	}
	if expiration == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrExpirationNotSet)).Send()
		return ErrExpirationNotSet
	}
	return c.read(ctx, key, modelPtr, expiration)
}
func (c *redisCache) read(ctx context.Context, key string, modelPtr interface{}, expirationTime *time.Duration) error {
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	if !c.IsValid(ctx) {
		return ErrCacheInvalid
	}
	var redisResult string
	var err error
	if expirationTime == nil {
		redisResult, err = c.redisClient.JSONGet(ctx, key).Result()
	} else {
		pipeline := c.redisClient.Pipeline()
		ttl := pipeline.TTL(ctx, key)
		jsonGet := pipeline.JSONGet(ctx, key)
		_, err = pipeline.Exec(ctx)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				return ErrItemNotFound
			}
			log.Error().Ctx(ctx).Err(err).Interface("key", key).Msg(c.fmtMsg("failed to execute pipeline"))
			return err
		}
		var expiration time.Duration
		expiration, err = ttl.Result()
		if err != nil || (expiration == -1 || expiration == -2) {
			log.Warn().Ctx(ctx).Err(err).Interface("key", key).Msg(c.fmtMsg("failed to get expiration"))
		} else {
			err = c.redisClient.Expire(ctx, key, *expirationTime).Err()
			if err != nil {
				log.Warn().Ctx(ctx).Err(err).Interface("key", key).Msg(c.fmtMsg("failed to update expiration"))
			}
		}
		redisResult, err = jsonGet.Result()
	}
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrItemNotFound
		}
		log.Warn().Ctx(ctx).Err(err).Interface("key", key).Msg(c.fmtMsg("getting json failed"))
		return err
	}
	err = json.Unmarshal([]byte(redisResult), modelPtr)
	if err != nil {
		log.Error().Ctx(ctx).Err(err).Interface("key", key).Msg(c.fmtMsg("unmarshal failed"))
		return err
	}
	return nil
}
func (c *redisCache) ReadGroup(ctx context.Context, keys []string, modelArrayPtr interface{}) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	if !c.IsValid(ctx) {
		return ErrCacheInvalid
	}
	redisResult, err := c.redisClient.JSONMGet(ctx, "$", keys...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrItemNotFound
		}
		log.Warn().Ctx(ctx).Err(err).Msg(c.fmtMsg("getting json failed"))
		return err
	}
	v := reflect.ValueOf(modelArrayPtr)
	if v.Kind() != reflect.Ptr {
		err = fmt.Errorf("modelArrayPtr must be a pointer")
		log.Error().Ctx(ctx).Err(c.fmtErr(err)).Send()
		return err
	}
	v = v.Elem()
	if v.Kind() != reflect.Slice {
		err = fmt.Errorf("modelArrayPtr must be a slice")
		log.Error().Ctx(ctx).Err(c.fmtErr(err)).Send()
		return err
	}
	elemType := v.Type().Elem()
	for i := range redisResult {
		newElem := reflect.New(elemType)
		if redisResult[i] == nil {
			log.Debug().Ctx(ctx).Interface("key", keys[i]).Msg(MsgItemNotFound)
			continue
		}
		err = json.Unmarshal([]byte(redisResult[i].(string)), newElem.Interface())
		if err != nil {
			log.Error().Ctx(ctx).Err(err).Interface("key", keys[i]).Msg(c.fmtMsg("unmarshal failed"))
			return err
		}
		v.Set(reflect.Append(v, newElem.Elem()))
	}
	return nil
}
func (c *redisCache) Delete(ctx context.Context, key string) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	return c.redisClient.Del(ctx, key).Err()
}

// Set handling

func (c *redisCache) AddItemToSet(ctx context.Context, key string, item string) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	return c.redisClient.SAdd(ctx, key, item).Err()
}
func (c *redisCache) IsItemInSet(ctx context.Context, key string, item string) (bool, error) {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return false, ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return false, ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return false, ErrNoClientSet
	}
	return c.redisClient.SIsMember(ctx, key, item).Result()
}
func (c *redisCache) GetItemsInSetAsMap(ctx context.Context, key string) (map[string]struct{}, error) {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return nil, ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return nil, ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return nil, ErrNoClientSet
	}
	return c.redisClient.SMembersMap(ctx, key).Result()
}
func (c *redisCache) DeleteItemFromSet(ctx context.Context, key string, item string) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	return c.redisClient.SRem(ctx, key, item).Err()
}

// Flag handling

func (c *redisCache) SetFlag(ctx context.Context, key string) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	return c.redisClient.Set(ctx, key, "", 0).Err()
}
func (c *redisCache) SetFlagWithExpiration(ctx context.Context, key string, expirationTime *time.Duration) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	expiration := c.config.DefaultExpiration
	if expirationTime != nil {
		expiration = expirationTime
	}
	return c.redisClient.Set(ctx, key, "", *expiration).Err()
}
func (c *redisCache) GetFlag(ctx context.Context, key string) (bool, error) {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return false, ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return false, ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return false, ErrNoClientSet
	}
	count, err := c.redisClient.Exists(ctx, key).Result()
	return count > 0, err
}
func (c *redisCache) DeleteFlag(ctx context.Context, key string) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	return c.redisClient.Del(ctx, key).Err()
}

// Index handling

func (c *redisCache) CreateIndex(ctx context.Context, index string, options *redis.FTCreateOptions, fieldSchemas []*redis.FieldSchema) (string, error) {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return "", ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return "", ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return "", ErrNoClientSet
	}
	return c.redisClient.FTCreate(ctx, index, options, fieldSchemas...).Result()
}
func (c *redisCache) SearchInIndex(ctx context.Context, indexName string, queryString string, options *redis.FTSearchOptions, modelArrayPtr interface{}) (totalCount int, err error) {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return 0, ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return 0, ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return 0, ErrNoClientSet
	}
	redisResult, err := c.redisClient.FTSearchWithArgs(ctx, indexName, queryString, options).Result()
	if err != nil {
		if strings.Contains(err.Error(), "No such index") {
			log.Error().Ctx(ctx).Err(err).Interface("index", indexName).Msg(c.fmtErr(ErrNoSuchIndexFound).Error())
			return 0, ErrNoSuchIndexFound
		}
		log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("searching in index failed"))
		return 0, err
	}
	v := reflect.ValueOf(modelArrayPtr)
	if v.Kind() != reflect.Ptr {
		err = fmt.Errorf("modelArrayPtr must be a pointer")
		log.Error().Ctx(ctx).Err(c.fmtErr(err)).Send()
		return 0, err
	}
	v = v.Elem()
	if v.Kind() != reflect.Slice {
		err = fmt.Errorf("modelArrayPtr must be a slice")
		log.Error().Ctx(ctx).Err(c.fmtErr(err)).Send()
		return 0, err
	}
	elemType := v.Type().Elem()
	for i := range redisResult.Docs {
		newElem := reflect.New(elemType)
		if err = json.Unmarshal([]byte(redisResult.Docs[i].Fields["$"]), newElem.Interface()); err != nil {
			log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("unmarshal failed"))
			return 0, err
		}
		v.Set(reflect.Append(v, newElem.Elem()))
	}
	var indexInfo redis.FTInfoResult
	indexInfo, err = c.redisClient.FTInfo(ctx, indexName).Result()
	if err != nil {
		log.Error().Ctx(ctx).Err(err).Msg(c.fmtMsg("getting index info failed"))
		return 0, err
	}
	return indexInfo.NumDocs, nil
}
func (c *redisCache) DeleteIndex(ctx context.Context, index string, deleteDocuments bool) error {
	if c.config == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrConfigNotSet)).Send()
		return ErrConfigNotSet
	}
	if c.config.IsDisabled {
		return ErrCachingDisabled
	}
	if c.redisClient == nil {
		log.Error().Ctx(ctx).Err(c.fmtErr(ErrNoClientSet)).Send()
		return ErrNoClientSet
	}
	return c.redisClient.FTDropIndexWithArgs(ctx, index, &redis.FTDropIndexOptions{DeleteDocs: deleteDocuments}).Err()
}

// Key handling

func (c *redisCache) KeyForAll() string {
	return fmt.Sprintf("%s:ALL", c.name)
}
func (c *redisCache) KeyForOne(id uuid.UUID) string {
	return fmt.Sprintf("%s:ONE:%s", c.name, GuidToKey(id))
}
func (c *redisCache) KeyForPage(page pagination.PaginatedQuery) string {
	return fmt.Sprintf("%s:PAGE:%d|%d|%s|%s", c.name, page.PageSize, page.Page, page.Direction, page.Sort)
}
func (c *redisCache) KeyForCustomPage(page pagination.PaginatedQuery, customKey string) string {
	return fmt.Sprintf("%s:%s", c.KeyForPage(page), customKey)
}
func (c *redisCache) KeyForCustom(customKey string) string {
	return fmt.Sprintf("%s:%s", c.name, customKey)
}
func (c *redisCache) KeyForValuedCustom(name string, values ...string) string {
	return fmt.Sprintf("%s:%s:", c.name, name) + strings.Join(values, "|")
}
func (c *redisCache) KeyForNotFound() string {
	return fmt.Sprintf("%s:NOT_FOUND", c.name)
}
func (c *redisCache) keyForSystem(key string) string {
	return fmt.Sprintf("%s:SYS:%s", c.name, key)
}

// Helper functions

func GuidToKey(id uuid.UUID) string {
	return normalizeKey(id.String())
}
func NameToKey(name string) string {
	return normalizeKey(name)
}
func normalizeKey(s string) string {
	s = strings.TrimSpace(s)
	replacer := strings.NewReplacer(
		" ", "_",
		"-", "_",
		"/", "_",
		"\\", "_",
		".", "_",
	)
	s = replacer.Replace(s)
	re := regexp.MustCompile(`[^a-zA-Z0-9_]`)
	s = re.ReplaceAllString(s, "")
	return s
}

// Internal helper functions

func (c *redisCache) fmtMsg(message string) string {
	return fmt.Sprintf("redisCache / %s: %s", c.name, message)
}
func (c *redisCache) fmtErr(err error) error {
	return fmt.Errorf("redisCache / %s: %w", c.name, err)
}
