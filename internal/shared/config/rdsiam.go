// RDS-IAM auth support for MustPgxPool (DB_AUTH=rds-iam). Ported from
// api-platform's internal/shared/database — mirror fixes across both repos.
package config

import (
	"context"
	"fmt"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	"github.com/jackc/pgx/v5"
)

// tokenSigner builds an RDS-IAM auth token. It matches
// feature/rds/auth.BuildAuthToken so production wires that function directly
// and tests substitute a deterministic stub (no AWS calls). The endpoint is
// "host:port"; dbUser is the Postgres role; creds is the SigV4 signing
// identity.
type tokenSigner func(ctx context.Context, endpoint, region, dbUser string, creds aws.CredentialsProvider) (string, error)

// awsConfigLoader matches config.LoadDefaultConfig minus the variadic optFns
// so tests can stub the region/credential source.
type awsConfigLoader func(ctx context.Context) (aws.Config, error)

// newRDSIAMBeforeConnect wires the production signer and the default AWS
// config chain (region from AWS_REGION, which Lambda always sets) into an
// rds-iam BeforeConnect hook.
func newRDSIAMBeforeConnect() func(context.Context, *pgx.ConnConfig) error {
	return rdsIAMBeforeConnect(
		// Wrap to drop BuildAuthToken's variadic optFns, which the fixed
		// tokenSigner signature (the test seam) doesn't carry.
		func(ctx context.Context, endpoint, region, dbUser string, creds aws.CredentialsProvider) (string, error) {
			return auth.BuildAuthToken(ctx, endpoint, region, dbUser, creds)
		},
		func(ctx context.Context) (aws.Config, error) {
			return awsconfig.LoadDefaultConfig(ctx)
		},
	)
}

// rdsIAMBeforeConnect returns a pgxpool BeforeConnect hook that mints a FRESH
// RDS-IAM token for every new physical connection and presents it as the
// password on the client→proxy leg. Tokens expire after 15 minutes, so a
// token baked into the pool config would start failing dials once the pool
// outlives it; per-connection minting is safe because signing is local SigV4
// over cached credentials — zero STS/IAM API calls on the connect path.
//
// The AWS config loads once (lazily, on the first dial — its provider chain
// caches and refreshes credentials internally) and is reused for every mint.
func rdsIAMBeforeConnect(sign tokenSigner, loadAWS awsConfigLoader) func(context.Context, *pgx.ConnConfig) error {
	var (
		once    sync.Once
		awsCfg  aws.Config
		loadErr error
	)
	return func(ctx context.Context, connCfg *pgx.ConnConfig) error {
		once.Do(func() { awsCfg, loadErr = loadAWS(ctx) })
		if loadErr != nil {
			return fmt.Errorf("config: load aws config for rds-iam auth: %w", loadErr)
		}
		endpoint := fmt.Sprintf("%s:%d", connCfg.Host, connCfg.Port)
		token, err := sign(ctx, endpoint, awsCfg.Region, connCfg.User, awsCfg.Credentials)
		if err != nil {
			return fmt.Errorf("config: build rds-iam token for %s@%s: %w", connCfg.User, endpoint, err)
		}
		connCfg.Password = token
		return nil
	}
}
