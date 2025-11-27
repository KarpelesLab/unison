# unison

A Go library for coalescing duplicate function calls. When multiple goroutines request the same key simultaneously, only one executes the function while others wait and receive the same result.

## Installation

```bash
go get github.com/KarpelesLab/unison
```

## Usage

```go
package main

import (
    "fmt"
    "github.com/KarpelesLab/unison"
)

var group unison.Group[string]

func main() {
    result, err := group.Do("my-key", func() (string, error) {
        // This function only runs once per key at a time
        return "computed value", nil
    })
    if err != nil {
        panic(err)
    }
    fmt.Println(result)
}
```

## How It Works

- Multiple concurrent calls with the same key will only execute the function once
- All callers receive the same result (value and error)
- Once the function completes and results are returned, subsequent calls will trigger a new execution
- Different keys execute independently and in parallel

## Example: Caching Expensive Operations

```go
var userLoader unison.Group[*User]

func GetUser(id string) (*User, error) {
    return userLoader.Do(id, func() (*User, error) {
        // Even if 100 goroutines call GetUser("123") simultaneously,
        // only one database query is made
        return db.QueryUser(id)
    })
}
```

## Example: Preventing Thundering Herd

```go
var cacheRefresh unison.Group[[]Product]

func GetProducts() ([]Product, error) {
    return cacheRefresh.Do("products", func() ([]Product, error) {
        // When cache expires and many requests hit at once,
        // only one request fetches from the source
        return fetchProductsFromAPI()
    })
}
```

## Example: Time-Based Caching with DoUntil

```go
var configLoader unison.Group[*Config]

func GetConfig() (*Config, error) {
    return configLoader.DoUntil("config", 5*time.Minute, func() (*Config, error) {
        // Result is cached for 5 minutes
        // Subsequent calls within that window return the cached result
        return loadConfigFromFile()
    })
}
```

## API

### `Group[T any]`

A generic type that manages in-flight function calls. Zero-value is ready to use.

```go
var g unison.Group[string]
```

### `(*Group[T]) Do(key string, fn func() (T, error)) (T, error)`

Executes `fn` for the given `key`, ensuring only one execution is in-flight at a time per key. Concurrent callers with the same key wait for the first caller's result. Once complete, subsequent calls trigger a new execution.

### `(*Group[T]) DoUntil(key string, dur time.Duration, fn func() (T, error)) (T, error)`

Like `Do`, but caches the result for the specified duration. Subsequent calls within the cache window return the cached result without executing `fn` again. After expiration, the next call triggers a new execution.

## License

See [LICENSE](LICENSE) file.
