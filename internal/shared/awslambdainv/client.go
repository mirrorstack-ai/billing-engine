// Package awslambdainv is billing-engine's thin wrapper around the AWS
// Lambda + CloudWatch APIs that cmd/infra-ssr-compute-sync depends on to
// enumerate the app-hosting SSR Lambda fleet ("ms-apphost-*") and pull each
// function's Duration/Invocations metrics.
//
// Mirrors internal/shared/cloudflare's shape (a small interface-backed
// surface + realClient/NewClient constructor, with tests substituting a
// fake) even though this wraps the real AWS SDK v2 clients directly rather
// than hand-rolling an HTTP client — the puller-vs-producer split (design
// doc docs-temp/app-hosting/ssr-metering-design.md §2) is the same shape
// either way, and tests here MUST NEVER call the real AWS APIs (same hard
// rule as cloudflare's).
//
// Direction is billing-engine → AWS (an OUTBOUND pull, read-only): no
// lambda:InvokeFunction, no cloudwatch:PutMetricData. See design doc §2.3 for
// why lambda:ListFunctions and cloudwatch:GetMetricData both require
// Resource:"*" (neither action supports resource-level IAM scoping) — the
// ms-apphost- filter lives entirely in ListSSRFunctions, not the IAM policy.
package awslambdainv

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/google/uuid"
)

// SSRFunction is one enumerated ms-apphost-* Lambda function: its AppID/Env
// (reverse-parsed from FunctionName, see ParseSSRFunctionName) and its
// configured MemorySize — read directly off the SAME ListFunctions page (no
// separate lambda:GetFunctionConfiguration call needed; MemorySize is already
// present on every returned FunctionConfiguration — design doc §2.1).
type SSRFunction struct {
	FunctionName string
	AppID        uuid.UUID
	Env          string
	MemoryMB     int32
}

// listFunctionsAPI is the narrow lambda.Client surface ListSSRFunctions
// depends on, so tests substitute a fake without a real AWS call.
type listFunctionsAPI interface {
	ListFunctions(ctx context.Context, params *lambda.ListFunctionsInput, optFns ...func(*lambda.Options)) (*lambda.ListFunctionsOutput, error)
}

// getMetricDataAPI is the narrow cloudwatch.Client surface GetMetricData
// depends on, so tests substitute a fake without a real AWS call.
type getMetricDataAPI interface {
	GetMetricData(ctx context.Context, params *cloudwatch.GetMetricDataInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricDataOutput, error)
}

// Client is the real AWS-backed implementation. Construct via NewClient.
type Client struct {
	lambdaAPI listFunctionsAPI
	cwAPI     getMetricDataAPI
}

// NewClient builds a Client from an already-loaded aws.Config (region +
// credentials resolve through the SDK's default chain — Lambda's own
// execution role in production, no separate secret needed: unlike the
// Cloudflare puller, there is no external API token to hold here).
func NewClient(cfg aws.Config) *Client {
	return &Client{
		lambdaAPI: lambda.NewFromConfig(cfg),
		cwAPI:     cloudwatch.NewFromConfig(cfg),
	}
}

// ListSSRFunctions paginates lambda:ListFunctions (no filter parameter exists
// on the API — AWS does not support server-side prefix filtering) and
// returns every function whose name carries the ms-apphost- prefix, with
// AppID/Env reverse-parsed (ParseSSRFunctionName) and MemoryMB read directly
// off the same ListFunctions page.
//
// A function whose name doesn't parse to a valid UUID+env suffix is skipped
// + logged (mirrors infra-egress-sync's unparseable-app_id skip) — never
// fatal, never panics.
func (c *Client) ListSSRFunctions(ctx context.Context) ([]SSRFunction, error) {
	var out []SSRFunction
	var marker *string
	for {
		page, err := c.lambdaAPI.ListFunctions(ctx, &lambda.ListFunctionsInput{Marker: marker})
		if err != nil {
			return nil, fmt.Errorf("lambda ListFunctions: %w", err)
		}
		for _, fn := range page.Functions {
			name := aws.ToString(fn.FunctionName)
			if !strings.HasPrefix(name, ssrFunctionPrefix) {
				continue
			}
			appID, env, ok := ParseSSRFunctionName(name)
			if !ok {
				slog.WarnContext(ctx, "skipping ms-apphost- function with unparseable name",
					"function_name", name)
				continue
			}
			out = append(out, SSRFunction{
				FunctionName: name,
				AppID:        appID,
				Env:          env,
				MemoryMB:     aws.ToInt32(fn.MemorySize),
			})
		}
		if page.NextMarker == nil {
			return out, nil
		}
		marker = page.NextMarker
	}
}

// GetMetricData is a thin pass-through to cloudwatch:GetMetricData that
// handles the API's own result pagination (NextToken) — the batching (<=250
// functions / 500 MetricDataQuery entries per call), the synthetic Id
// scheme, and the Id-based result correlation are all the CALLER's
// responsibility (cmd/infra-ssr-compute-sync/query.go), kept out of this
// thin AWS wrapper so that logic is unit-testable without any AWS SDK types
// crossing into a fake network call.
func (c *Client) GetMetricData(ctx context.Context, queries []cwtypes.MetricDataQuery, start, end time.Time) ([]cwtypes.MetricDataResult, error) {
	var results []cwtypes.MetricDataResult
	var nextToken *string
	for {
		out, err := c.cwAPI.GetMetricData(ctx, &cloudwatch.GetMetricDataInput{
			MetricDataQueries: queries,
			StartTime:         aws.Time(start),
			EndTime:           aws.Time(end),
			NextToken:         nextToken,
		})
		if err != nil {
			return nil, fmt.Errorf("cloudwatch GetMetricData: %w", err)
		}
		results = append(results, out.MetricDataResults...)
		if out.NextToken == nil {
			return results, nil
		}
		nextToken = out.NextToken
	}
}
