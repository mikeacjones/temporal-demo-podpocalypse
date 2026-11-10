package api

import (
	"embed"
	"net/http"
	"strings"
)

//go:embed web/index.html web/join.html web/assets/*
var web embed.FS

func (s *Server) index(w http.ResponseWriter, _ *http.Request) {
	body, err := web.ReadFile("web/index.html")
	if err != nil {
		http.Error(w, "booth page is missing", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(body)
}

// joinPage is the phone form behind the booth's QR code.
func (s *Server) joinPage(w http.ResponseWriter, _ *http.Request) {
	body, err := web.ReadFile("web/join.html")
	if err != nil {
		http.Error(w, "join page is missing", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(body)
}

func (s *Server) asset(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	body, err := web.ReadFile("web/assets/" + name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", assetType(name))
	w.Header().Set("Cache-Control", "public, max-age=86400")
	_, _ = w.Write(body)
}

func assetType(name string) string {
	switch {
	case strings.HasSuffix(name, ".mp3"):
		return "audio/mpeg"
	case strings.HasSuffix(name, ".png"):
		return "image/png"
	case strings.HasSuffix(name, ".js"):
		return "text/javascript; charset=utf-8"
	case strings.HasSuffix(name, ".css"):
		return "text/css; charset=utf-8"
	case strings.HasSuffix(name, ".txt"):
		return "text/plain; charset=utf-8"
	default:
		return "application/octet-stream"
	}
}
