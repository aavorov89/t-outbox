package httpapi

import (
	"encoding/json"
	"net/http"
)

// ProblemContentType is the media type of RFC 9457 problem details.
const ProblemContentType = "application/problem+json"

// problemTypeBase points to the human-readable docs of each problem type. RFC 9457 wants
// "type" to be a URI that identifies the problem; clients match on it, never on "title".
const problemTypeBase = "https://github.com/aavorov89/t-outbox/blob/main/docs/api-errors.md#"

// Problem type URIs. Generic HTTP errors use "about:blank", for which RFC 9457 says the
// title is the status phrase.
const (
	TypeMalformedRequest       = problemTypeBase + "malformed-request"
	TypeValidation             = problemTypeBase + "validation-error"
	TypeIdempotencyKeyMissing  = problemTypeBase + "idempotency-key-missing"
	TypeIdempotencyKeyReused   = problemTypeBase + "idempotency-key-reused"
	TypeInvalidStateTransition = problemTypeBase + "invalid-state-transition"
	TypeAboutBlank             = "about:blank"
)

// Problem is an RFC 9457 problem details object. Errors is an extension member used by
// validation-error.
type Problem struct {
	Type     string       `json:"type"`
	Title    string       `json:"title"`
	Status   int          `json:"status"`
	Detail   string       `json:"detail,omitempty"`
	Instance string       `json:"instance,omitempty"`
	Errors   []FieldError `json:"errors,omitempty"`
}

// FieldError points at one invalid field. Pointer is a JSON Pointer (RFC 6901) into the
// request body, as RFC 9457's own example does.
type FieldError struct {
	Pointer string `json:"pointer"`
	Detail  string `json:"detail"`
}

func writeProblem(w http.ResponseWriter, r *http.Request, p Problem) {
	if p.Type == "" {
		p.Type = TypeAboutBlank
	}
	if p.Title == "" {
		p.Title = http.StatusText(p.Status)
	}
	p.Instance = r.URL.Path
	w.Header().Set("Content-Type", ProblemContentType)
	w.WriteHeader(p.Status)
	_ = json.NewEncoder(w).Encode(p)
}

func writeStatus(w http.ResponseWriter, r *http.Request, status int, detail string) {
	writeProblem(w, r, Problem{Status: status, Detail: detail})
}
