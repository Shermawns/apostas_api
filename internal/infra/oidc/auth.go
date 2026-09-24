package oidc

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"time"

	"apostas_api/internal/infra/config"
	"github.com/golang-jwt/jwt/v5"
)

type principalKey struct{}
type Principal struct{ ClientID string }

func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(principalKey{}).(Principal)
	return p, ok
}

type Auth struct {
	cfg    config.Config
	client *http.Client
}

func NewAuth(cfg config.Config) *Auth {
	return &Auth{cfg: cfg, client: &http.Client{Timeout: 5 * time.Second}}
}

func (a *Auth) Ready(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.JWKSURL, nil)
	if err != nil {
		return err
	}
	res, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return errors.New("OIDC JWKS unavailable")
	}
	var keys struct {
		Keys []json.RawMessage `json:"keys"`
	}
	if err := json.NewDecoder(res.Body).Decode(&keys); err != nil {
		return err
	}
	if len(keys.Keys) == 0 {
		return errors.New("OIDC JWKS has no keys")
	}
	return nil
}

func (a *Auth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/health/") {
			next.ServeHTTP(w, r)
			return
		}
		parts := strings.Fields(r.Header.Get("Authorization"))
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			http.Error(w, "missing bearer token", http.StatusUnauthorized)
			return
		}
		claims := jwt.MapClaims{}
		token, err := jwt.ParseWithClaims(parts[1], claims, func(t *jwt.Token) (any, error) {
			if t.Method.Alg() != jwt.SigningMethodRS256.Alg() {
				return nil, errors.New("unsupported signing algorithm")
			}
			kid, ok := t.Header["kid"].(string)
			if !ok {
				return nil, errors.New("missing key ID")
			}
			return a.key(r.Context(), kid)
		}, jwt.WithIssuer(a.cfg.IssuerURL), jwt.WithAudience(a.cfg.Audience), jwt.WithExpirationRequired())
		if err != nil || !token.Valid {
			http.Error(w, "invalid bearer token", http.StatusUnauthorized)
			return
		}
		clientID, ok := claims["azp"].(string)
		if !ok || clientID == "" {
			http.Error(w, "missing client identity", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), principalKey{}, Principal{ClientID: clientID})))
	})
}

func (a *Auth) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.cfg.JWKSURL, nil)
	if err != nil {
		return nil, err
	}
	res, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return nil, errors.New("JWKS unavailable")
	}
	var keys struct {
		Keys []struct {
			Kid string `json:"kid"`
			Kty string `json:"kty"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if err := json.NewDecoder(res.Body).Decode(&keys); err != nil {
		return nil, err
	}
	for _, key := range keys.Keys {
		if key.Kid != kid || key.Kty != "RSA" {
			continue
		}
		nBytes, err := base64.RawURLEncoding.DecodeString(key.N)
		if err != nil {
			return nil, err
		}
		eBytes, err := base64.RawURLEncoding.DecodeString(key.E)
		if err != nil {
			return nil, err
		}
		e := new(big.Int).SetBytes(eBytes)
		if !e.IsInt64() || e.Int64() < 3 || e.Int64() > 1<<31-1 {
			return nil, errors.New("invalid exponent")
		}
		return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: int(e.Int64())}, nil
	}
	return nil, errors.New("signing key not found")
}
