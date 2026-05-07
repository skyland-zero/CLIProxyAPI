package usage

import (
	"fmt"
	"strings"
	"time"

	_ "time/tzdata"
)

var summaryTimeZoneAliases = map[string]struct {
	name   string
	offset int
}{
	"asia/shanghai": {name: "Asia/Shanghai", offset: 8 * 60 * 60},
	"prc":           {name: "PRC", offset: 8 * 60 * 60},
}

func ResolveSummaryTimeZone(value string) (string, *time.Location, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "UTC", time.UTC, nil
	}
	if location, err := time.LoadLocation(value); err == nil {
		return value, location, nil
	}
	if alias, ok := summaryTimeZoneAliases[strings.ToLower(value)]; ok {
		return alias.name, time.FixedZone(alias.name, alias.offset), nil
	}
	if normalized, location, ok := parseFixedOffsetTimeZone(value); ok {
		return normalized, location, nil
	}
	return "", nil, fmt.Errorf("invalid tz")
}

func summaryLocation(timeZone string) *time.Location {
	_, location, err := ResolveSummaryTimeZone(timeZone)
	if err != nil {
		return time.UTC
	}
	return location
}

func normalizeSummaryTimeZone(value string) string {
	normalized, _, err := ResolveSummaryTimeZone(value)
	if err != nil {
		return "UTC"
	}
	return normalized
}

func parseFixedOffsetTimeZone(value string) (string, *time.Location, bool) {
	if len(value) != 6 || (value[0] != '+' && value[0] != '-') || value[3] != ':' {
		return "", nil, false
	}
	hours, errHours := parseTwoDigits(value[1:3])
	minutes, errMinutes := parseTwoDigits(value[4:6])
	if errHours != nil || errMinutes != nil || hours > 23 || minutes > 59 {
		return "", nil, false
	}
	offset := hours*60*60 + minutes*60
	if value[0] == '-' {
		offset = -offset
	}
	return value, time.FixedZone(value, offset), true
}

func parseTwoDigits(value string) (int, error) {
	if len(value) != 2 {
		return 0, fmt.Errorf("invalid two-digit number")
	}
	if value[0] < '0' || value[0] > '9' || value[1] < '0' || value[1] > '9' {
		return 0, fmt.Errorf("invalid two-digit number")
	}
	return int(value[0]-'0')*10 + int(value[1]-'0'), nil
}
