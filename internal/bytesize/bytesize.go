package bytesize

import (
	"errors"
	"math"
	"strconv"
	"strings"
)

type Size uint64

const (
	B  Size = 1
	KB Size = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
)

var unitMap = map[string]Size{
	"B":     B,
	"BYTE":  B,
	"BYTES": B,

	"KB":        KB,
	"KILOBYTE":  KB,
	"KILOBYTES": KB,

	"MB":        MB,
	"MEGABYTE":  MB,
	"MEGABYTES": MB,

	"GB":        GB,
	"GIGABYTE":  GB,
	"GIGABYTES": GB,

	"TB":        TB,
	"TERABYTE":  TB,
	"TERABYTES": TB,

	"PB":        PB,
	"PETABYTE":  PB,
	"PETABYTES": PB,

	"EB":       EB,
	"EXABYTE":  EB,
	"EXABYTES": EB,
}

var (
	ErrInvalidNumberFormat = errors.New("invalid number format")
	ErrUnknownUnit         = errors.New("unknown unit")
	ErrInvalidSizeFormat   = errors.New("invalid size format")
)

var ErrTooLarge = errors.New("size exceeds max int64 bytes")

// ParseSize parses sizes such as "512kb" or "1.5 MB". A unit is required.
func ParseSize(sizeStr string) (Size, error) {
	sizeStr = strings.TrimSpace(sizeStr)
	split := strings.IndexFunc(sizeStr, func(c rune) bool { return (c < '0' || c > '9') && c != '.' })
	if split < 0 {
		return 0, ErrInvalidSizeFormat // no unit
	}
	numberStr, unitStr := sizeStr[:split], strings.ToUpper(strings.TrimSpace(sizeStr[split:]))

	number, err := strconv.ParseFloat(numberStr, 64)
	if err != nil {
		return 0, ErrInvalidNumberFormat
	}
	unitSize, ok := unitMap[unitStr]
	if !ok {
		return 0, ErrUnknownUnit
	}
	bytes := number * float64(unitSize)
	if bytes >= math.MaxInt64 {
		return 0, ErrTooLarge
	}
	return Size(bytes), nil
}
