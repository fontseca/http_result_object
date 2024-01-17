package main

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"log"
	"math"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"
)

type User struct {
	ID         uuid.UUID  `json:"id"`
	FirstName  string     `json:"first_name"`
	MiddleName string     `json:"middle_name"`
	LastName   string     `json:"last_name"`
	Surname    string     `json:"surname"`
	Age        int        `json:"age"`
	JobTitle   string     `json:"job_title"`
	Country    string     `json:"country"`
	City       string     `json:"city"`
	Address    string     `json:"address"`
	TZ         string     `json:"tz"`
	PictureURL string     `json:"picture_url"`
	Phone      string     `json:"phone"`
	Company    string     `json:"company"`
	Password   string     `json:"password"`
	Bio        string     `json:"bio"`
	Email      string     `json:"email"`
	Gender     string     `json:"gender"`
	IPAddress  string     `json:"ip_address"`
	ArchivedAt *time.Time `json:"archived_at"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  time.Time  `json:"updated_at"`
}

type Result[T any] struct {
	Page    int64 `json:"page"`
	RPP     int64 `json:"rpp"`
	Payload []T   `json:"payload"`
}

// sanitizedField represents a pair of strings where the first string is the snake_case version
// of a field and the second string is the corresponding PascalCase version.
type sanitizedField [2]string

// snake returns the snake_case version of the sanitizedField.
func (s sanitizedField) snake() string {
	return s[0]
}

// pascal returns the PascalCase version of the sanitizedField.
func (s sanitizedField) pascal() string {
	return s[1]
}

var contains = func(haystack []sanitizedField, needle string) bool {
	return slices.ContainsFunc(haystack, func(s sanitizedField) bool {
		return needle == s.snake()
	})
}

// sanitizeFields takes a variable number of field names and returns a slice of sanitized fields.
// If T == reflect.Struct, it returns a []sanitizedField with the snake and pascal version of each field.
// If T == reflect.Map, it returns a []sanitizedField only with the snake version of each field.
func sanitizeFields[T any](fields ...string) []sanitizedField {
	var dummy T
	var t = reflect.TypeOf(dummy)
	var sanitizedFields []sanitizedField
	switch t.Kind() {
	default:
		return sanitizedFields
	case reflect.Struct:
		for _, field := range fields {
			field = strings.Trim(field, "\n\a\b\f\r\t\v ")
			if "" == field {
				continue
			}
			for i := 0; i < t.NumField(); i++ {
				var sanitized = sanitizedField{}
				var record = t.Field(i)
				var tagValue = strings.Trim(strings.Split(record.Tag.Get("json"), ",")[0], " ")
				sanitized[0] = tagValue
				sanitized[1] = record.Name
				if sanitized.snake() == field && !contains(sanitizedFields, sanitized.snake()) {
					sanitizedFields = append(sanitizedFields, sanitized)
				}
			}
		}
	case reflect.Map:
		for _, key := range fields {
			key = strings.Trim(key, "\n\a\b\f\r\t\v ")
			if "" != key && !contains(sanitizedFields, key) {
				var sanitized = sanitizedField{}
				sanitized[0] = key
				sanitizedFields = append(sanitizedFields, sanitized)
			}
		}
	}
	return sanitizedFields
}

func computeDelta(payloadSize int) int {
	var size = float64(payloadSize)
	switch {
	case size <= 1 || size <= math.Pow(10, 2):
		return 1
	case size <= math.Pow(10, 3):
		return 2
	case size <= math.Pow(10, 4):
		return 3
	default:
		return 4
	}
}

func doTransform[T any](wg *sync.WaitGroup, kind reflect.Kind, out *[]map[string]any, in []T, fields []sanitizedField) {
	defer wg.Done()
	for _, entry := range in {
		var entryValue = reflect.ValueOf(entry)
		var entryFields = map[string]any{}
		for _, field := range fields {
			if reflect.Struct == kind {
				var value = entryValue.FieldByName(field.pascal())
				if value.IsValid() {
					entryFields[field.snake()] = value.Interface()
				}
			} else if reflect.Map == kind {
				for _, key := range entryValue.MapKeys() {
					var value = entryValue.MapIndex(key)
					if field.snake() == key.String() {
						entryFields[field.snake()] = value.Interface()
					}
				}
			}
		}
		*out = append(*out, entryFields)
	}
}

// With transforms the result payload by selecting only the specified fields.
func (r Result[T]) With(fields ...string) *Result[map[string]any] {
	var transformed = &Result[map[string]any]{Page: r.Page, RPP: r.RPP, Payload: make([]map[string]any, 0)}
	var payloadSize = len(r.Payload)
	if 0 == len(fields) || nil == r.Payload || 0 == payloadSize {
		return transformed
	}
	var kind = reflect.TypeOf(r.Payload[0]).Kind()
	if reflect.Struct != kind && reflect.Map != kind {
		return transformed
	}
	var delta = computeDelta(payloadSize)
	var chunkSize = payloadSize / delta
	var chunks = make([][]map[string]any, delta)
	for i := 0; i < delta; i++ {
		chunks[i] = make([]map[string]any, 0, chunkSize)
	}
	transformed.Payload = make([]map[string]any, 0, payloadSize)
	var wg = sync.WaitGroup{}
	wg.Add(delta)
	var sanitizedFields = sanitizeFields[T](fields...)
	for i := 0; i < delta; i++ {
		var end = (1 + i) * chunkSize
		if 1+i == delta {
			end = payloadSize
		}
		go doTransform[T](&wg, kind, &chunks[i], r.Payload[i*chunkSize:end], sanitizedFields)
	}
	wg.Wait()
	for i := 0; i < delta; i++ {
		transformed.Payload = append(transformed.Payload, chunks[i]...)
	}
	return transformed
}

func parseUsersFromDisk(source string) *Result[User] {
	fmt.Printf("Parsing mock data from %q... ", source)
	var start = time.Now()
	var b, err = os.ReadFile(source)
	if nil != err {
		log.Fatal(err)
	}
	var payload []User
	err = json.Unmarshal(b, &payload)
	if nil != err {
		log.Fatal(err)
	}
	fmt.Printf("Done. Parsed %d records in %.3fs\n", len(payload), time.Now().Sub(start).Seconds())
	return &Result[User]{Page: 1, RPP: int64(len(payload)), Payload: payload}
}

func main() {
	var r = parseUsersFromDisk("mock_data.json")
	var fields = []string{
		"first_name",
		"\a\b\ncompany\n",
		"first_name",
		"first_name",
		"\t\nfirst_name\t",
		"first_name\n",
		"archived_at",
		"middle_name",
		"last_name",
		"surname",
		"picture_url",
		"trashed_at",
		"",
		"\ntz\n",
		"created_at",
		"address",
		"phone_number",
		"phone",
		"email",
		"ip_address",
		"id",
	}
	fmt.Print("Performing partial... ")
	var start = time.Now()
	var res = r.With(fields...)
	fmt.Printf("Done in %.3fs\n", time.Now().Sub(start).Seconds())
	var n = len(res.Payload) - 1
	fmt.Printf("Record %d in payload is:\n", n)
	fmt.Println("Partial:")
	var record, _ = json.MarshalIndent(res.Payload[n], "", "  ")
	fmt.Println(string(record))
	fmt.Println("Original:")
	record, _ = json.MarshalIndent(r.Payload[n], "", "  ")
	fmt.Println(string(record))
}
