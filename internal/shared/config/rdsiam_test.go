// In-package tests (package config, not config_test): they exercise the
// unexported pgxPoolConfig / rdsIAMBeforeConnect seams directly — nothing
// here touches os.Exit, AWS, or a live database.
package config

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/jackc/pgx/v5"
)

// TestPgxPoolConfigPasswordMode: the default ("" and "password") modes must
// leave the parsed pool config untouched — no BeforeConnect hook, no error —
// so rds-iam support cannot perturb local dev.
func TestPgxPoolConfigPasswordMode(t *testing.T) {
	for _, mode := range []string{"", AuthPassword} {
		t.Run("mode="+mode, func(t *testing.T) {
			poolCfg, err := pgxPoolConfig("postgres://mirrorstack:mirrorstack@localhost:5432/mirrorstack?sslmode=disable", mode)
			if err != nil {
				t.Fatalf("pgxPoolConfig() error = %v, want nil", err)
			}
			if poolCfg.BeforeConnect != nil {
				t.Error("BeforeConnect is set, want nil in password mode")
			}
			if poolCfg.ConnConfig.User != "mirrorstack" || poolCfg.ConnConfig.Password != "mirrorstack" {
				t.Errorf("parsed user/password = %q/%q, want mirrorstack/mirrorstack",
					poolCfg.ConnConfig.User, poolCfg.ConnConfig.Password)
			}
		})
	}
}

// TestPgxPoolConfigUnknownMode: a typo'd DB_AUTH must fail loudly, never
// silently fall back to password auth.
func TestPgxPoolConfigUnknownMode(t *testing.T) {
	_, err := pgxPoolConfig("postgres://billing_svc:@proxy.example.com:5432/mirrorstack?sslmode=require", "iam")
	if err == nil || !strings.Contains(err.Error(), "unknown DB_AUTH") {
		t.Fatalf("pgxPoolConfig() error = %v, want unknown DB_AUTH error", err)
	}
}

// TestPgxPoolConfigRDSIAMRequiresTLS: the sslmode=disable + rds-iam
// combination must fail loudly at config time, before any dial.
func TestPgxPoolConfigRDSIAMRequiresTLS(t *testing.T) {
	_, err := pgxPoolConfig("postgres://billing_svc:@proxy.example.com:5432/mirrorstack?sslmode=disable", AuthRDSIAM)
	if err == nil {
		t.Fatal("pgxPoolConfig() with rds-iam + sslmode=disable succeeded, want error")
	}
	if !strings.Contains(err.Error(), "sslmode=require") {
		t.Errorf("pgxPoolConfig() error = %v, want mention of sslmode=require", err)
	}
}

// TestPgxPoolConfigRDSIAMWiresHook: with TLS on, rds-iam mode must install a
// BeforeConnect hook (the token minter) on the parsed config.
func TestPgxPoolConfigRDSIAMWiresHook(t *testing.T) {
	poolCfg, err := pgxPoolConfig("postgres://billing_svc:@proxy.example.com:5432/mirrorstack?sslmode=require", AuthRDSIAM)
	if err != nil {
		t.Fatalf("pgxPoolConfig() error = %v, want nil", err)
	}
	if poolCfg.BeforeConnect == nil {
		t.Fatal("BeforeConnect is nil, want rds-iam token hook")
	}
}

func testConnConfig(t *testing.T) *pgx.ConnConfig {
	t.Helper()
	connCfg, err := pgx.ParseConfig("postgres://billing_svc:@proxy.example.com:5432/mirrorstack")
	if err != nil {
		t.Fatalf("pgx.ParseConfig: %v", err)
	}
	return connCfg
}

// TestRDSIAMBeforeConnectMintsFreshToken drives the hook directly with a
// stubbed signer: every new connection gets a freshly minted token as its
// password, while the AWS config loads exactly once.
func TestRDSIAMBeforeConnectMintsFreshToken(t *testing.T) {
	var gotEndpoint, gotRegion, gotUser string
	mints := 0
	sign := func(ctx context.Context, endpoint, region, dbUser string, creds aws.CredentialsProvider) (string, error) {
		mints++
		gotEndpoint, gotRegion, gotUser = endpoint, region, dbUser
		return fmt.Sprintf("stub-token-%d", mints), nil
	}
	loads := 0
	loadAWS := func(ctx context.Context) (aws.Config, error) {
		loads++
		return aws.Config{Region: "ap-northeast-1", Credentials: aws.AnonymousCredentials{}}, nil
	}

	hook := rdsIAMBeforeConnect(sign, loadAWS)
	connCfg := testConnConfig(t)

	if err := hook(context.Background(), connCfg); err != nil {
		t.Fatalf("hook: %v", err)
	}
	if connCfg.Password != "stub-token-1" {
		t.Errorf("Password = %q, want %q", connCfg.Password, "stub-token-1")
	}
	if gotEndpoint != "proxy.example.com:5432" {
		t.Errorf("signer endpoint = %q, want %q", gotEndpoint, "proxy.example.com:5432")
	}
	if gotRegion != "ap-northeast-1" {
		t.Errorf("signer region = %q, want %q", gotRegion, "ap-northeast-1")
	}
	if gotUser != "billing_svc" {
		t.Errorf("signer dbUser = %q, want %q", gotUser, "billing_svc")
	}

	// A second dial mints a fresh token but must not reload the AWS config.
	if err := hook(context.Background(), connCfg); err != nil {
		t.Fatalf("hook (2nd dial): %v", err)
	}
	if connCfg.Password != "stub-token-2" {
		t.Errorf("Password after 2nd dial = %q, want %q", connCfg.Password, "stub-token-2")
	}
	if loads != 1 {
		t.Errorf("AWS config loads = %d, want 1", loads)
	}
}

func TestRDSIAMBeforeConnectSignError(t *testing.T) {
	sign := func(ctx context.Context, endpoint, region, dbUser string, creds aws.CredentialsProvider) (string, error) {
		return "", errors.New("boom")
	}
	loadAWS := func(ctx context.Context) (aws.Config, error) {
		return aws.Config{Region: "ap-northeast-1"}, nil
	}

	hook := rdsIAMBeforeConnect(sign, loadAWS)
	connCfg := testConnConfig(t)

	err := hook(context.Background(), connCfg)
	if err == nil || !strings.Contains(err.Error(), "rds-iam token") {
		t.Fatalf("hook error = %v, want wrapped rds-iam token error", err)
	}
	if connCfg.Password != "" {
		t.Errorf("Password = %q, want untouched on error", connCfg.Password)
	}
}
