package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"podpocalypse/internal/config"
)

func TestPlatformIdentityHeaders(t *testing.T) {
	s := &Server{Config: config.Config{TrustIdentityHeaders: true}}
	for header, want := range map[string]string{
		"Ada@temporal.io":       "ada@temporal.io",
		"ada@example.com":       "",
		"Ada <ada@temporal.io>": "",
		"":                      "",
	} {
		r := httptest.NewRequest(http.MethodGet, "/api/kiosk", nil)
		r.Header.Set("X-Temporal-Auth-Email", header)
		got, err := s.employee(r)
		if err != nil || got != want {
			t.Fatalf("header %q: got %q, %v; want %q", header, got, err, want)
		}
	}
	// Untrusted, the same header is ignored.
	r := httptest.NewRequest(http.MethodGet, "/api/kiosk", nil)
	r.Header.Set("X-Temporal-Auth-Email", "ada@temporal.io")
	if got, _ := (&Server{}).employee(r); got != "" {
		t.Fatalf("untrusted header signed in %q", got)
	}
}

func TestPlatformVerifier(t *testing.T) {
	var sent http.Header
	verifier := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sent = r.Header.Clone()
		switch r.Header.Get("Cookie") {
		case "temporal_demo_auth_live=ok; other=1":
			w.Header().Set("X-Temporal-Auth-Email", "ada@temporal.io")
			w.WriteHeader(http.StatusAccepted)
		case "temporal_demo_auth_live=down":
			w.WriteHeader(http.StatusBadGateway)
		default:
			w.WriteHeader(http.StatusUnauthorized)
		}
	}))
	defer verifier.Close()
	s := &Server{Config: config.Config{PlatformVerifyURL: verifier.URL, PlatformCookie: "temporal_demo_auth_live", TrustIdentityHeaders: true}}

	ask := func(cookie string) (string, error) {
		r := httptest.NewRequest(http.MethodGet, "/api/kiosk", nil)
		r.Header.Set("X-Forwarded-Uri", "/secret")
		if cookie != "" {
			r.Header.Set("Cookie", cookie)
		}
		return s.employee(r)
	}
	if got, err := ask("temporal_demo_auth_live=ok; other=1"); err != nil || got != "ada@temporal.io" {
		t.Fatalf("signed in: %q %v", got, err)
	}
	if sent.Get("X-Forwarded-Uri") != "" {
		t.Fatal("only the Cookie header goes to the verifier")
	}
	if got, err := ask("temporal_demo_auth_live=stale"); err != nil || got != "" {
		t.Fatalf("anonymous: %q %v", got, err)
	}
	if _, err := ask("temporal_demo_auth_live=down"); err == nil {
		t.Fatal("a failing verifier is an outage, not anonymous")
	}
	sent = nil
	if got, err := ask(""); err != nil || got != "" || sent != nil {
		t.Fatal("no cookies, no verifier call")
	}
}

func TestSignInReturnsToTheBooth(t *testing.T) {
	s := &Server{Config: config.Config{PlatformLoginURL: "https://auth.tmprl-demo.cloud/oauth2/start?rd=", PublicURL: "https://podpocalypse.tmprl-demo.cloud"}}
	rr := httptest.NewRecorder()
	s.signIn(rr, httptest.NewRequest(http.MethodGet, "/signin", nil))
	want := "https://auth.tmprl-demo.cloud/oauth2/start?rd=https%3A%2F%2Fpodpocalypse.tmprl-demo.cloud%2F"
	if rr.Code != http.StatusFound || rr.Header().Get("Location") != want {
		t.Fatalf("got %d %q", rr.Code, rr.Header().Get("Location"))
	}
}
