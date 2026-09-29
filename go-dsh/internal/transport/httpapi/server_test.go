package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yy59750901/go-dsh/internal/transport/httpapi"
)

func TestHealth(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	response := httptest.NewRecorder()
	httpapi.NewHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusOK)
	}
}
