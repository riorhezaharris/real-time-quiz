// Package quiz models quizzes and questions. The question bank is a mocked
// dependency: in production it would be a separate content service or
// database, so it sits behind the Repository interface.
package quiz

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

// ErrNotFound is returned when a quiz ID does not exist.
var ErrNotFound = errors.New("quiz not found")

// Question is a multiple-choice vocabulary question. Answer is never sent to
// clients; correctness is always evaluated on the server.
type Question struct {
	ID      string   `json:"id"`
	Prompt  string   `json:"prompt"`
	Options []string `json:"options"`
	Answer  string   `json:"answer"`
}

// IsCorrect compares an answer case-insensitively, ignoring surrounding space.
func (q Question) IsCorrect(answer string) bool {
	return strings.EqualFold(strings.TrimSpace(answer), strings.TrimSpace(q.Answer))
}

// Quiz is an ordered set of questions identified by a unique quiz ID.
type Quiz struct {
	ID        string     `json:"id"`
	Title     string     `json:"title"`
	Questions []Question `json:"questions"`

	byID map[string]int
}

// Question looks up a question by ID in O(1).
func (q *Quiz) Question(id string) (Question, bool) {
	i, ok := q.byID[id]
	if !ok {
		return Question{}, false
	}
	return q.Questions[i], true
}

func (q *Quiz) index() error {
	q.byID = make(map[string]int, len(q.Questions))
	for i, qs := range q.Questions {
		if qs.ID == "" || qs.Answer == "" {
			return fmt.Errorf("quiz %q: question %d missing id or answer", q.ID, i)
		}
		if _, dup := q.byID[qs.ID]; dup {
			return fmt.Errorf("quiz %q: duplicate question id %q", q.ID, qs.ID)
		}
		q.byID[qs.ID] = i
	}
	return nil
}

// Repository provides read access to quizzes.
type Repository interface {
	Get(ctx context.Context, quizID string) (*Quiz, error)
}

// MemoryRepository is an immutable, concurrency-safe in-memory Repository.
type MemoryRepository struct {
	quizzes map[string]*Quiz
}

// NewMemoryRepository indexes the given quizzes. The quizzes must not be
// mutated afterwards.
func NewMemoryRepository(quizzes ...*Quiz) (*MemoryRepository, error) {
	r := &MemoryRepository{quizzes: make(map[string]*Quiz, len(quizzes))}
	for _, q := range quizzes {
		if err := q.index(); err != nil {
			return nil, err
		}
		if _, dup := r.quizzes[q.ID]; dup {
			return nil, fmt.Errorf("duplicate quiz id %q", q.ID)
		}
		r.quizzes[q.ID] = q
	}
	return r, nil
}

// Get implements Repository.
func (r *MemoryRepository) Get(_ context.Context, quizID string) (*Quiz, error) {
	q, ok := r.quizzes[quizID]
	if !ok {
		return nil, ErrNotFound
	}
	return q, nil
}

// LoadJSON decodes a JSON array of quizzes.
func LoadJSON(r io.Reader) ([]*Quiz, error) {
	var quizzes []*Quiz
	if err := json.NewDecoder(r).Decode(&quizzes); err != nil {
		return nil, fmt.Errorf("decode quizzes: %w", err)
	}
	return quizzes, nil
}

//go:embed fixtures/quizzes.json
var fixtures []byte

// NewFixtureRepository returns a repository seeded with the bundled mock
// vocabulary quizzes.
func NewFixtureRepository() (*MemoryRepository, error) {
	quizzes, err := LoadJSON(bytes.NewReader(fixtures))
	if err != nil {
		return nil, err
	}
	return NewMemoryRepository(quizzes...)
}
