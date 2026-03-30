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
	return FTSScoreWithOptions(text, terms, 0, 0)
}

func FTSScoreWithOptions(text string, terms []string, maxDistance uint32, minTermLength uint32) float32 {
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
		normalized := strings.ToLower(term)
		best := freq[normalized]
		if maxDistance > 0 && uint32(len([]rune(normalized))) >= minTermLength {
			for token, count := range freq {
				if token == normalized {
					continue
				}
				if levenshteinDistance(normalized, token) <= int(maxDistance) && count > best {
					best = count
				}
			}
		}
		score += float32(best)
	}
	return score
}

func levenshteinDistance(left string, right string) int {
	leftRunes := []rune(left)
	rightRunes := []rune(right)
	if len(leftRunes) == 0 {
		return len(rightRunes)
	}
	if len(rightRunes) == 0 {
		return len(leftRunes)
	}

	prev := make([]int, len(rightRunes)+1)
	curr := make([]int, len(rightRunes)+1)
	for j := range prev {
		prev[j] = j
	}
	for i, leftRune := range leftRunes {
		curr[0] = i + 1
		for j, rightRune := range rightRunes {
			cost := 0
			if leftRune != rightRune {
				cost = 1
			}
			deletion := prev[j+1] + 1
			insertion := curr[j] + 1
			substitution := prev[j] + cost
			curr[j+1] = minInt(deletion, insertion, substitution)
		}
		prev, curr = curr, prev
	}
	return prev[len(rightRunes)]
}

func minInt(values ...int) int {
	best := values[0]
	for _, value := range values[1:] {
		if value < best {
			best = value
		}
	}
	return best
}
