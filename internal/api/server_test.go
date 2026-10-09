package api

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"podpocalypse/internal/workflows"
)

func TestApplyReturning(t *testing.T) {
	profile := workflows.PlayerState{Name: "Ada", Title: "SA", Consent: true}
	got, err := applyReturning(PlayRequest{Email: "ada@temporal.io"}, profile, true)
	if err != nil || got.Name != "Ada" || got.Title != "SA" || !got.Consent {
		t.Fatalf("returning player: %+v %v", got, err)
	}
	got, err = applyReturning(PlayRequest{Email: "ada@temporal.io", Name: "Ada Lovelace"}, profile, true)
	if err != nil || got.Name != "Ada Lovelace" || got.Title != "SA" {
		t.Fatalf("partial form: %+v %v", got, err)
	}
	if _, err := applyReturning(PlayRequest{Email: "new@temporal.io"}, profile, false); err == nil {
		t.Fatal("a first flight still needs a name and title")
	}
}

func TestValidatePlay(t *testing.T) {
	ok := PlayRequest{Name: "Ada", Title: "SA", Email: "ada@temporal.io", Consent: true}
	if err := ValidatePlay(ok); err != nil {
		t.Fatal(err)
	}
	cases := []PlayRequest{
		{Title: "SA", Email: "ada@temporal.io"},
		{Name: "Ada", Email: "ada@temporal.io"},
		{Name: "Ada", Title: "SA", Email: "not-an-email"},
		{Name: "Ada", Title: "SA", Email: "ada@temporal.io", DurationSeconds: 5},
	}
	for _, tc := range cases {
		if err := ValidatePlay(tc); err == nil {
			t.Fatalf("expected error for %+v", tc)
		}
	}
}

func TestIndexPage(t *testing.T) {
	rr := httptest.NewRecorder()
	(&Server{}).index(rr, httptest.NewRequest(http.MethodGet, "/", nil))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), "Kill the worker") {
		t.Fatalf("index status %d body %s", rr.Code, rr.Body.String())
	}
}
