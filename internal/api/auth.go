package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/mail"
	"net/url"
	"strings"
	"time"
)

// employee returns the signed-in temporal.io employee at this request, or ""
// for anyone else. In order:
//
//   - the local stand-in, DEV_AUTH_EMAIL;
//   - on the field demo platform, the X-Temporal-Auth-Email header its edge sets
//     on an `auth: optional` host (the edge strips any the client sent first);
//   - on the field demo platform, its verifier, sent the whole Cookie header;
//   - on the catalog, its verifier, sent the temporal_demo_auth cookie.
func (s *Server) employee(r *http.Request) (string, error) {
	if s.Config.DevAuthEmail != "" {
		return s.Config.DevAuthEmail, nil
	}
	if s.Config.TrustIdentityHeaders {
		if email := r.Header.Get("X-Temporal-Auth-Email"); email != "" {
			return employeeEmail(email), nil
		}
	}
	if s.Config.PlatformVerifyURL != "" {
		return s.platformEmployee(r)
	}
	if s.Config.AuthVerifyURL != "" {
		return s.catalogEmployee(r)
	}
	return "", nil
}

const authTimeout = 3 * time.Second

// maxCookieHeader bounds the Cookie header sent to the platform's verifier
// (oauth2-proxy splits a large session into a few 4 KB cookies).
const maxCookieHeader = 16384

// platformEmployee asks the platform's verifier (FIELDKIT_AUTH_VERIFY_URL)
// whom the request's cookies belong to. It sends the Cookie header and nothing
// else. Any 2xx with X-Temporal-Auth-Email is signed in; a 2xx without it, a
// redirect, 401 or 403 is anonymous; anything else is an outage.
func (s *Server) platformEmployee(r *http.Request) (string, error) {
	cookies := strings.Join(r.Header.Values("Cookie"), "; ")
	if cookies == "" || len(cookies) > maxCookieHeader {
		return "", nil
	}
	ctx, cancel := context.WithTimeout(r.Context(), authTimeout)
	defer cancel()
	verify, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Config.PlatformVerifyURL, nil)
	if err != nil {
		return "", err
	}
	verify.Header.Set("Cookie", cookies)
	response, err := authClient.Do(verify)
	if err != nil {
		slog.Warn("platform sign-in verifier unavailable", "error", err)
		return "", err
	}
	defer response.Body.Close()
	switch code := response.StatusCode; {
	case code >= 200 && code < 300:
		return employeeEmail(response.Header.Get("X-Temporal-Auth-Email")), nil
	case code >= 300 && code < 400, code == http.StatusUnauthorized, code == http.StatusForbidden:
		return "", nil
	default:
		// Whether the session cookie was sent helps diagnosis; its value is never logged.
		slog.Warn("platform sign-in verifier failed", "status", code,
			"sessionCookieSent", s.Config.PlatformCookie != "" && strings.Contains(cookies, s.Config.PlatformCookie))
		return "", errors.New("platform sign-in verifier failed")
	}
}

// catalogEmployee asks the tmprl-demo.cloud catalog's verifier (AUTH_VERIFY_URL)
// whom the temporal_demo_auth cookie belongs to. Only a 204 is signed in.
func (s *Server) catalogEmployee(r *http.Request) (string, error) {
	cookie, err := r.Cookie("temporal_demo_auth")
	if err != nil || cookie.Value == "" || len(cookie.Value) > 8192 {
		return "", nil
	}
	origin, err := url.Parse(s.Config.PublicURL)
	if err != nil || origin.Host == "" || (origin.Scheme != "https" && origin.Scheme != "http") {
		return "", errors.New("public url is not configured")
	}
	ctx, cancel := context.WithTimeout(r.Context(), authTimeout)
	defer cancel()
	verify, err := http.NewRequestWithContext(ctx, http.MethodGet, s.Config.AuthVerifyURL, nil)
	if err != nil {
		return "", err
	}
	verify.AddCookie(&http.Cookie{Name: cookie.Name, Value: cookie.Value})
	// The configured origin, never caller-supplied forwarding headers.
	verify.Header.Set("X-Forwarded-Host", origin.Host)
	verify.Header.Set("X-Forwarded-Proto", origin.Scheme)
	verify.Header.Set("X-Forwarded-Method", r.Method)
	verify.Header.Set("X-Forwarded-Uri", r.URL.RequestURI())
	response, err := authClient.Do(verify)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return "", nil
	}
	subject := strings.TrimSpace(response.Header.Get("X-Temporal-Auth-Subject"))
	if subject == "" || subject == "bootstrap-catalog-auth" {
		return "", nil
	}
	return employeeEmail(response.Header.Get("X-Temporal-Auth-Email")), nil
}

// employeeEmail returns email, lowercased, when it is a plain temporal.io
// address, and "" otherwise.
func employeeEmail(email string) string {
	address, err := mail.ParseAddress(email)
	if err != nil || address.Address != email {
		return ""
	}
	local, domain, found := strings.Cut(email, "@")
	if !found || local == "" || !strings.EqualFold(domain, "temporal.io") {
		return ""
	}
	return strings.ToLower(email)
}

// authClient never follows redirects: a redirect from a verifier means
// "not signed in", not a page to fetch.
var authClient = &http.Client{
	Timeout:       authTimeout,
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}

// signIn sends an employee through the platform's sign-in and back to the
// booth's front page. The booth screen shows no sign-in link, because
// attendees use it; an employee opens /signin.
func (s *Server) signIn(w http.ResponseWriter, r *http.Request) {
	if s.Config.PlatformLoginURL == "" || s.Config.PublicURL == "" {
		http.Redirect(w, r, "/", http.StatusFound)
		return
	}
	back := strings.TrimRight(s.Config.PublicURL, "/") + "/"
	http.Redirect(w, r, s.Config.PlatformLoginURL+url.QueryEscape(back), http.StatusFound)
}
