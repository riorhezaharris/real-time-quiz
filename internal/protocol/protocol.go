// Package protocol defines the JSON messages exchanged over the WebSocket.
// It is shared by the server and the load generator.
//
// Every message is a JSON object with a "type" discriminator.
//
//	client → server: join, answer
//	server → client: joined, answer_result, leaderboard, error
package protocol

// Message types.
const (
	TypeJoin         = "join"
	TypeAnswer       = "answer"
	TypeJoined       = "joined"
	TypeAnswerResult = "answer_result"
	TypeLeaderboard  = "leaderboard"
	TypeError        = "error"
)

// Error codes sent in Error.Code.
const (
	CodeBadRequest      = "bad_request"
	CodeNotJoined       = "not_joined"
	CodeAlreadyJoined   = "already_joined"
	CodeQuizNotFound    = "quiz_not_found"
	CodeUnknownQuestion = "unknown_question"
	CodeRateLimited     = "rate_limited"
	CodeInternal        = "internal"
)

// ClientMessage is the union of all client → server messages; which fields
// are meaningful depends on Type.
type ClientMessage struct {
	Type string `json:"type"`

	// join
	QuizID      string `json:"quizId,omitempty"`
	UserID      string `json:"userId,omitempty"`
	DisplayName string `json:"displayName,omitempty"`

	// answer
	RequestID  string `json:"requestId,omitempty"`
	QuestionID string `json:"questionId,omitempty"`
	Answer     string `json:"answer,omitempty"`
}

// Question is a question as shown to clients (without the answer).
type Question struct {
	ID      string   `json:"id"`
	Prompt  string   `json:"prompt"`
	Options []string `json:"options"`
}

// LeaderboardEntry is one leaderboard row.
type LeaderboardEntry struct {
	UserID      string `json:"userId"`
	DisplayName string `json:"displayName"`
	Score       int64  `json:"score"`
	Rank        int64  `json:"rank"`
}

// Joined confirms a join and carries everything needed to render the quiz.
type Joined struct {
	Type        string             `json:"type"`
	QuizID      string             `json:"quizId"`
	Title       string             `json:"title"`
	UserID      string             `json:"userId"`
	Questions   []Question         `json:"questions"`
	Score       int64              `json:"score"`
	Rank        int64              `json:"rank"`
	Leaderboard []LeaderboardEntry `json:"leaderboard"`
}

// AnswerResult acknowledges an answer with the user's updated standing.
type AnswerResult struct {
	Type            string `json:"type"`
	RequestID       string `json:"requestId,omitempty"`
	QuestionID      string `json:"questionId"`
	Correct         bool   `json:"correct"`
	AlreadyAnswered bool   `json:"alreadyAnswered"`
	Score           int64  `json:"score"`
	Rank            int64  `json:"rank"`
}

// Leaderboard is a full snapshot of the top of a quiz's standings. Because
// each message is a complete snapshot, a lost message is healed by the next.
type Leaderboard struct {
	Type         string             `json:"type"`
	QuizID       string             `json:"quizId"`
	Version      int64              `json:"version"`
	ServerTimeMs int64              `json:"serverTimeMs"`
	Participants int64              `json:"participants"`
	Top          []LeaderboardEntry `json:"top"`
}

// Error reports a problem with a client message. RequestID echoes the
// offending answer's request ID when available.
type Error struct {
	Type      string `json:"type"`
	RequestID string `json:"requestId,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
}
