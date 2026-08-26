# cache
Contains the `RedisCache` class for easy interaction with Redis. It is a fully integrated standalone cache solution tailored for bloodlab usage.

###### Install
`go get github.com/blutspende/libs/cache`

## New
A new instance can be created calling `NewRedisCache`:
```go
func NewRedisCache(redisClient *redis.Client, appName, cacheName string) RedisCache
```
It requires a pre-configured `*redis.Client` from the `github.com/redis/go-redis/v9` package, and the application name and a name of the cache instance.
It is important that the names are unique for each service instantiating RedisCache to avoid collisions, as it is used as a combination `appName:cacheName` to prefix all keys stored in the cache.
Avoid using spaces or underscores in the names, as they are not allowed in Redis keys. Dashes are allowed.

## Init
After creating the `Init` method should be called to initialize the cache.
```go
func (c *redisCache) Init(config RedisCacheConfig, refreshFillerFunc func(ctx context.Context) error, refreshInitFunc func(ctx context.Context) error)
```
RedisCache has built-in support for refreshing with automated retry policy, and custom filler and init functions, which can be provided in the init.
Calling `Init` can be omitted if neither refresh nor any of the config's functions are used. But be cautious, as these functions will produce errors if called without initialization.

## Config
The `RedisCacheConfig` struct is used to configure the cache instance.
```go
type RedisCacheConfig struct {
    RefreshRetryAttempts     int
    RefreshRetryWaitStartMs  int
    RefreshRetryWaitExponent int
    DefaultExpiration        *time.Duration
    MultiserverMode          bool
    MutexExpiration          *time.Duration
    IsDisabled               bool
}
```
Refresh parameters are used to configure the retry policy for the refresh mechanism. Refresh starts with `RefreshRetryWaitStartMs` milliseconds wait time, and increases the wait time exponentially by `RefreshRetryWaitExponent` for each retry, up to `RefreshRetryAttempts` retries.
`DefaultExpiration` is used in `...WithExpiration` functions if explicit expiration is not provided.
`MultiserverMode` enables multiserver support, which allows multiple programs (or multiple instances of the same program) to simultaneously access the same redis cache without causing issues.
`MutexExpiration` is used to set the expiration time for mutex locks used in multiserver mode, to avoid permanently locked states if an instance crashes while holding a lock.
`IsDisabled` can be used to disable the cache, avoiding any interaction with the cache (saving time for development and testing), and returns an error in any operation is attempted. `IsValid` always returns false in this case.

## Refreshing and validity
Cache has an internally stored validity state, which can be checked with `IsValid` method. If the cache is invalid, it should be refreshed with `RefreshCacheAsync` method, it is not done automatically, but calling any read operation will result in error. The cache can be actively invalidated with `SetToInvalid` method, or manually set to valid (without calling `RefreshCacheAsync`) with `SetToValid` method if needed.
```go
func RefreshCacheAsync(ctx context.Context, forceUpdate bool)
```
Can be called to refresh the cache asynchronously, using the filler and init functions provided in the `Init` method.
If `forceUpdate` is set to true, the cache will be refreshed even if another refresh is already in progress, after that is finished. If is useful if the cache is known to be stale, and needs to be updated as soon as possible (e.g.: after create of update events). If `forceUpdate` is false, and a refresh is already in progress, the call won't do anything.

## CRUD
The cache provides basic CRUD operations for storing and retrieving data using keys and an underlying JSON format.
```go
Store(ctx context.Context, key string, content interface{}) error
StoreWithExpiration(ctx context.Context, key string, content interface{}, expirationTime *time.Duration) error
Read(ctx context.Context, key string, modelPtr interface{}) error
ReadWithExpiration(ctx context.Context, key string, modelPtr interface{}, expirationTime *time.Duration) error
ReadGroup(ctx context.Context, keys []string, modelArrayPtr interface{}) error
Delete(ctx context.Context, key string) error
```
Note: The key should ALWAYS be used by generating `KeyFor...` functions provided by RedisCache!

## Other functions
There are some additional functions provided for specific use cases.
```go
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
```

## Key generation
To ensure consistent key generation, RedisCache provides functions to generate keys for different purposes. They all use the cache instance name as prefix.
```go
KeyForAll() string
KeyForOne(id uuid.UUID) string
KeyForPage(page pagination.Pagination) string
KeyForCustomPage(page pagination.Pagination, customKey string) string
KeyForCustom(customKey string) string
KeyForValuedCustom(name string, values ...string) string
KeyForNotFound() string
```

## Helper functions
Additional helper functions are provided for key formatting. It is ill-advised in Redis to use dashes `-`, spaces and special characters in keys. It should also be all lower case as per convention. UUIDs naturally have dashes, so they should be reformatted. Application and cache names should also be normalized. For this these two helper functions are provided, and they should be used whenever a name is not made sure to be compliant as is.
```go
GuidToKey(id uuid.UUID) string
NameToKey(name string) string
```