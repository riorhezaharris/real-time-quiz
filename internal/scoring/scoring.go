// Package scoring owns every write to quiz state: registering participants
// and scoring answers. Redis is the single source of truth, so any number of
// stateless server instances can score answers for the same quiz concurrently.
package scoring

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"

	"github.com/riorhezaharris/real-time-quiz/internal/keys"
	"github.com/riorhezaharris/real-time-quiz/internal/quiz"
)

var (
	ErrInvalidID       = errors.New("ids must be 1-64 chars of [A-Za-z0-9_.-]")
	ErrInvalidName     = errors.New("display name must be 1-32 characters")
	ErrUnknownQuestion = errors.New("unknown question")
)

// idPattern keeps IDs safe to embed in Redis keys; in particular it forbids
// '{' and '}' which would break the cluster hash tag.
var idPattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// ValidateID reports whether s is an acceptable quiz, user or question ID.
func ValidateID(s string) error {
	if !idPattern.MatchString(s) {
		return ErrInvalidID
	}
	return nil
}

// Result is the outcome of a single answer submission.
type Result struct {
	// Correct is true only when this submission was scored and was correct.
	Correct bool
	// AlreadyAnswered is true when the user had already submitted an answer
	// to this question; the submission is ignored and the score unchanged.
	AlreadyAnswered bool
	// Score and Rank are the user's standing after the submission. Rank uses
	// standard competition ranking (1, 2, 2, 4): tied users share a rank.
	Score int64
	Rank  int64
}

// Service scores answers against Redis.
type Service struct {
	rdb    redis.Scripter
	repo   quiz.Repository
	points int64
	ttl    time.Duration
}

// New constructs a Service. points is awarded per correct answer and ttl
// bounds how long quiz state survives after the last write.
func New(rdb redis.Scripter, repo quiz.Repository, points int64, ttl time.Duration) *Service {
	return &Service{rdb: rdb, repo: repo, points: points, ttl: ttl}
}

// AI-ASSISTED (Claude Code): the Lua scripts below were drafted by Claude
// from the prompt "atomic, idempotent per-(user,question) scoring in Redis
// that works on Redis Cluster". Verified by: scoring_test.go (duplicate
// submits, 100 concurrent goroutines scoring exactly once under -race, tie
// ranks) and manual EVAL runs against redis-server 8.2. Refined by hand to
// return the rank from inside the script (saves a round trip) and to refresh
// TTLs on every write.

// joinScript registers a participant with 0 points if new and records their
// display name. KEYS: lb, names. ARGV: userID, name, ttlSeconds, channel.
// Returns {score, rank}.
var joinScript = redis.NewScript(`
local added = redis.call('ZADD', KEYS[1], 'NX', 0, ARGV[1])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
redis.call('EXPIRE', KEYS[2], ARGV[3])
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
local rank = redis.call('ZCOUNT', KEYS[1], '(' .. score, '+inf') + 1
if added == 1 then
  redis.call('PUBLISH', ARGV[4], ARGV[1])
end
return {score, rank}
`)

// answerScript applies an answer exactly once per (user, question).
// KEYS: answered, lb. ARGV: questionID, points (0 = incorrect), userID,
// ttlSeconds, channel. Returns {applied(0|1), score, rank}.
var answerScript = redis.NewScript(`
local applied = redis.call('SADD', KEYS[1], ARGV[1])
if applied == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[4])
  if tonumber(ARGV[2]) > 0 then
    redis.call('ZINCRBY', KEYS[2], ARGV[2], ARGV[3])
    redis.call('EXPIRE', KEYS[2], ARGV[4])
    redis.call('PUBLISH', ARGV[5], ARGV[3])
  else
    redis.call('ZADD', KEYS[2], 'NX', 0, ARGV[3])
  end
end
local score = redis.call('ZSCORE', KEYS[2], ARGV[3]) or '0'
local rank = redis.call('ZCOUNT', KEYS[2], '(' .. score, '+inf') + 1
return {applied, score, rank}
`)

// Join registers userID in quizID (idempotent) and returns their current
// standing. Re-joining after a reconnect keeps the existing score.
func (s *Service) Join(ctx context.Context, quizID, userID, name string) (score, rank int64, err error) {
	if err := ValidateID(quizID); err != nil {
		return 0, 0, err
	}
	if err := ValidateID(userID); err != nil {
		return 0, 0, err
	}
	if n := utf8.RuneCountInString(name); n == 0 || n > 32 {
		return 0, 0, ErrInvalidName
	}
	if _, err := s.repo.Get(ctx, quizID); err != nil {
		return 0, 0, err
	}
	res, err := joinScript.Run(ctx, s.rdb,
		[]string{keys.Leaderboard(quizID), keys.Names(quizID)},
		userID, name, s.ttlSeconds(), keys.EventsChannel(quizID),
	).Slice()
	if err != nil {
		return 0, 0, fmt.Errorf("join script: %w", err)
	}
	if len(res) != 2 {
		return 0, 0, fmt.Errorf("join script: unexpected reply %v", res)
	}
	if score, err = toInt(res[0]); err != nil {
		return 0, 0, err
	}
	if rank, err = toInt(res[1]); err != nil {
		return 0, 0, err
	}
	return score, rank, nil
}

// SubmitAnswer evaluates the answer server-side and applies it atomically.
// Only the first submission per (user, question) counts, so client retries
// and duplicate messages can never inflate a score.
func (s *Service) SubmitAnswer(ctx context.Context, quizID, userID, questionID, answer string) (Result, error) {
	if err := ValidateID(quizID); err != nil {
		return Result{}, err
	}
	if err := ValidateID(userID); err != nil {
		return Result{}, err
	}
	q, err := s.repo.Get(ctx, quizID)
	if err != nil {
		return Result{}, err
	}
	question, ok := q.Question(questionID)
	if !ok {
		return Result{}, ErrUnknownQuestion
	}

	correct := question.IsCorrect(answer)
	var points int64
	if correct {
		points = s.points
	}

	res, err := answerScript.Run(ctx, s.rdb,
		[]string{keys.Answered(quizID, userID), keys.Leaderboard(quizID)},
		questionID, points, userID, s.ttlSeconds(), keys.EventsChannel(quizID),
	).Slice()
	if err != nil {
		return Result{}, fmt.Errorf("answer script: %w", err)
	}
	if len(res) != 3 {
		return Result{}, fmt.Errorf("answer script: unexpected reply %v", res)
	}
	applied, err := toInt(res[0])
	if err != nil {
		return Result{}, err
	}
	score, err := toInt(res[1])
	if err != nil {
		return Result{}, err
	}
	rank, err := toInt(res[2])
	if err != nil {
		return Result{}, err
	}
	return Result{
		Correct:         applied == 1 && correct,
		AlreadyAnswered: applied == 0,
		Score:           score,
		Rank:            rank,
	}, nil
}

func (s *Service) ttlSeconds() int64 {
	secs := int64(s.ttl / time.Second)
	if secs <= 0 {
		secs = 1
	}
	return secs
}

// toInt converts a Lua reply element (integer or numeric string) to int64.
// Scores are always whole numbers because points are integers.
func toInt(v any) (int64, error) {
	switch t := v.(type) {
	case int64:
		return t, nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		if err != nil {
			return 0, fmt.Errorf("parse %q: %w", t, err)
		}
		return int64(f), nil
	default:
		return 0, fmt.Errorf("unexpected reply type %T", v)
	}
}
