package httpapi

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// NewHandler builds the public HTTP surface. Agent state transitions must stay
// behind application services; handlers only validate and translate requests.
func NewHandler() http.Handler {
	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery())
	router.GET("/healthz", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	return router
}
