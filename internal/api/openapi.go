package api

import "net/http"

const openAPISpec = `{
  "openapi": "3.0.3",
  "info": {
    "title": "Koinos Token Tracker API",
    "description": "Lightweight blockchain indexer for the Koinos network. Tracks addresses, token balances (KOIN/VHP), holder rankings, and transfer history. Runs as a microservice alongside the Koinos node.",
    "version": "0.1.0",
    "contact": {
      "name": "Koinos Indexer",
      "url": "https://github.com/koinos/koinos-token-tracker"
    }
  },
  "servers": [
    {
      "url": "/",
      "description": "This server"
    }
  ],
  "paths": {
    "/v1/indexer/status": {
      "get": {
        "summary": "Indexer sync status",
        "description": "Returns the current sync progress, last indexed block, and holder counts.",
        "tags": ["Status"],
        "responses": {
          "200": {
            "description": "Sync status",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "last_indexed_height": {"type": "integer", "example": 34500000},
                    "last_block_id": {"type": "string"},
                    "total_addresses": {"type": "integer", "example": 383000},
                    "koin_holders": {"type": "integer", "example": 9183},
                    "vhp_holders": {"type": "integer", "example": 194}
                  }
                }
              }
            }
          }
        }
      }
    },
    "/v1/indexer/stats": {
      "get": {
        "summary": "Chain statistics",
        "description": "Aggregate statistics including total addresses, holder counts, and tracked tokens.",
        "tags": ["Status"],
        "responses": {
          "200": {
            "description": "Chain stats"
          }
        }
      }
    },
    "/v1/indexer/addresses": {
      "get": {
        "summary": "List all addresses",
        "description": "Paginated list of all addresses ever seen on the Koinos blockchain.",
        "tags": ["Addresses"],
        "parameters": [
          {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 50, "maximum": 1000}},
          {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}}
        ],
        "responses": {
          "200": {
            "description": "Paginated address list"
          }
        }
      }
    },
    "/v1/indexer/address/{address}": {
      "get": {
        "summary": "Address details",
        "description": "Returns address info including first seen block and all token balances.",
        "tags": ["Addresses"],
        "parameters": [
          {"name": "address", "in": "path", "required": true, "schema": {"type": "string"}, "example": "14f34kUNZugf2DK4hPJGy4AkmBM7Y4pvVu"}
        ],
        "responses": {
          "200": {
            "description": "Address details with balances"
          },
          "404": {
            "description": "Address not found"
          }
        }
      }
    },
    "/v1/indexer/holders/{token}": {
      "get": {
        "summary": "Top token holders",
        "description": "Ranked list of token holders sorted by balance descending.",
        "tags": ["Tokens"],
        "parameters": [
          {"name": "token", "in": "path", "required": true, "schema": {"type": "string"}, "description": "Token contract address", "example": "19GYjDBVXU7keLbYvMLazsGQn3GTWHjHkK"},
          {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 50, "maximum": 1000}},
          {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}}
        ],
        "responses": {
          "200": {
            "description": "Paginated holder list"
          }
        }
      }
    },
    "/v1/indexer/transfers/{address}": {
      "get": {
        "summary": "Token transfer history",
        "description": "Paginated list of KOIN/VHP transfers, mints, and burns involving the given address.",
        "tags": ["Transfers"],
        "parameters": [
          {"name": "address", "in": "path", "required": true, "schema": {"type": "string"}, "example": "14f34kUNZugf2DK4hPJGy4AkmBM7Y4pvVu"},
          {"name": "limit", "in": "query", "schema": {"type": "integer", "default": 50, "maximum": 500}},
          {"name": "offset", "in": "query", "schema": {"type": "integer", "default": 0}}
        ],
        "responses": {
          "200": {
            "description": "Paginated transfer history"
          }
        }
      }
    },
    "/v1/indexer/blocks": {
      "get": {
        "summary": "Block metadata",
        "description": "Returns block metadata (producer, tx count, timestamp) for a height range.",
        "tags": ["Blocks"],
        "parameters": [
          {"name": "from", "in": "query", "schema": {"type": "integer"}},
          {"name": "to", "in": "query", "schema": {"type": "integer"}}
        ],
        "responses": {
          "200": {
            "description": "Block list"
          }
        }
      }
    },
    "/v1/indexer/tokens": {
      "get": {
        "summary": "Tracked tokens",
        "description": "List of all tracked token contracts.",
        "tags": ["Tokens"],
        "responses": {
          "200": {
            "description": "Token list"
          }
        }
      }
    }
  },
  "tags": [
    {"name": "Status", "description": "Indexer sync status and chain statistics"},
    {"name": "Addresses", "description": "Address discovery and lookup"},
    {"name": "Tokens", "description": "Token holders and metadata"},
    {"name": "Transfers", "description": "Token transfer history"},
    {"name": "Blocks", "description": "Block metadata"}
  ]
}`

const swaggerHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Koinos Token Tracker API</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
  <style>
    body { margin: 0; padding: 0; }
    .swagger-ui .topbar { display: none; }
  </style>
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    SwaggerUIBundle({
      url: '/openapi.json',
      dom_id: '#swagger-ui',
      deepLinking: true,
      presets: [SwaggerUIBundle.presets.apis, SwaggerUIBundle.SwaggerUIStandalonePreset],
      layout: "BaseLayout"
    });
  </script>
</body>
</html>`

func (h *handlers) handleOpenAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(openAPISpec))
}

func (h *handlers) handleDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(swaggerHTML))
}

func (h *handlers) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(explorerHTML))
}
