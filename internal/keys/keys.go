// Package keys centralizes the Redis key schema.
//
// Every key belonging to a quiz embeds the quiz ID in a hash tag ({quizID})
// so that, on Redis Cluster, all of a quiz's keys land on the same slot. That
// is what allows the scoring Lua script to touch several keys atomically.
package keys

// Leaderboard is a sorted set: member = userID, score = points.
func Leaderboard(quizID string) string { return "quiz:{" + quizID + "}:lb" }

// Names is a hash: field = userID, value = display name.
func Names(quizID string) string { return "quiz:{" + quizID + "}:names" }

// Answered is a set of question IDs a user has already submitted. It is the
// idempotency guard that makes scoring exactly-once per (user, question).
func Answered(quizID, userID string) string {
	return "quiz:{" + quizID + "}:answered:" + userID
}

// EventsChannel is the pub/sub channel announcing that a quiz's leaderboard
// changed. The payload is informational only (the user ID that changed).
func EventsChannel(quizID string) string { return "quiz:{" + quizID + "}:events" }

const eventsPrefix, eventsSuffix = "quiz:{", "}:events"

// QuizFromChannel extracts the quiz ID from an events channel name.
func QuizFromChannel(channel string) (string, bool) {
	if len(channel) <= len(eventsPrefix)+len(eventsSuffix) ||
		channel[:len(eventsPrefix)] != eventsPrefix ||
		channel[len(channel)-len(eventsSuffix):] != eventsSuffix {
		return "", false
	}
	return channel[len(eventsPrefix) : len(channel)-len(eventsSuffix)], true
}
