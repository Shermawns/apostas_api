package config

import (
	"errors"
	"os"
	"time"
)

type Config struct {
	Addr            string
	DatabaseURL     string
	IssuerURL       string
	JWKSURL         string
	Audience        string
	ShutdownTimeout time.Duration
	AWSRegion       string
	AWSEndpoint     string
	AWSAccessKey    string
	AWSSecretKey    string
	InputQueueURL   string
	OutputQueueURL  string
}

func Load() (Config, error) {
	c := Config{
		Addr: os.Getenv("APP_ADDR"), DatabaseURL: os.Getenv("DATABASE_URL"),
		IssuerURL: os.Getenv("OIDC_ISSUER_URL"), JWKSURL: os.Getenv("OIDC_JWKS_URL"), Audience: os.Getenv("OIDC_AUDIENCE"),
		AWSRegion: os.Getenv("AWS_REGION"), AWSEndpoint: os.Getenv("AWS_ENDPOINT_URL"),
		AWSAccessKey: os.Getenv("AWS_ACCESS_KEY_ID"), AWSSecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		InputQueueURL: os.Getenv("AWS_SQS_INPUT_QUEUE_URL"), OutputQueueURL: os.Getenv("AWS_SQS_OUTPUT_QUEUE_URL"),
	}
	if c.Addr == "" || c.DatabaseURL == "" || c.IssuerURL == "" || c.JWKSURL == "" || c.Audience == "" || c.AWSRegion == "" || c.InputQueueURL == "" || c.OutputQueueURL == "" {
		return Config{}, errors.New("missing required configuration")
	}
	var err error
	c.ShutdownTimeout, err = time.ParseDuration(os.Getenv("APP_SHUTDOWN_TIMEOUT"))
	if err != nil || c.ShutdownTimeout <= 0 {
		return Config{}, errors.New("invalid APP_SHUTDOWN_TIMEOUT")
	}
	return c, nil
}
