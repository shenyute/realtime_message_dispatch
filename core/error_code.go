package core

type Error struct {
	text      string
	temporary bool
	Err       error
}

// Fatal errors. They won't be recovered automatically after a while.
var InvalidCommandError = &Error{text: "Invalid command", temporary: false}
var UnauthorizedError = &Error{text: "UNAUTHORIZED", temporary: false}
var MessageTypeUnsupportedError = &Error{text: "Message type unsupported", temporary: false}
var CommandTypeUnsupportedError = &Error{text: "Command type unsupported", temporary: false}
var ClientClosedError = &Error{text: "Client closed", temporary: false}
var ProxyConnectFailError = &Error{text: "Proxy connected fail", temporary: false}
var ReplyIdIncorrect = &Error{text: "Reply Id incorrect", temporary: false}
var DuplicateNodeInfo = &Error{text: "DuplicateNodeInfo", temporary: false}

// Temporary errors.
var Timeout = &Error{text: "timeout", temporary: true}
var ClientRateLimit = &Error{text: "ratelimit", temporary: true}

func NewIOError(err error) *Error {
	return &Error{text: "io error", temporary: false, Err: err}
}

func (e *Error) Error() string {
	return e.text
}

func (e *Error) IsTemporary() bool {
	return e.temporary
}
