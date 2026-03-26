package search

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"unicode"
)

func FirstVectorProperty(props map[string]any) ([]float32, bool) {
	if len(props) == 0 {
		return nil, false
	}
	keys := make([]string, 0, len(props))
	for key := range props {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		if vector, ok := props[key].([]float32); ok {
			return append([]float32(nil), vector...), true
		}
	}
	return nil, false
}

func VectorDistance(left []float32, right []float32) (float32, error) {
	if len(left) != len(right) {
		return 0, fmt.Errorf("vector length mismatch: %d != %d", len(left), len(right))
	}
	total := float64(0)
	for i := range left {
		diff := float64(left[i] - right[i])
		total += diff * diff
	}
	return float32(math.Sqrt(total)), nil
}

func Tokenize(text string) []string {
	parts := strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func FTSScore(text string, terms []string) float32 {
	if len(terms) == 0 {
		return 0
	}
	tokens := Tokenize(text)
	if len(tokens) == 0 {
		return 0
	}
	freq := map[string]int{}
	for _, token := range tokens {
		freq[token]++
	}
	score := float32(0)
	for _, term := range terms {
		score += float32(freq[strings.ToLower(term)])
	}
	return score
}
