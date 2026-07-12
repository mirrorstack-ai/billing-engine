package awslambdainv

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
)

type fakeListFunctionsAPI struct {
	pages [][]lambdatypes.FunctionConfiguration
	calls int
}

func (f *fakeListFunctionsAPI) ListFunctions(_ context.Context, params *lambda.ListFunctionsInput, _ ...func(*lambda.Options)) (*lambda.ListFunctionsOutput, error) {
	idx := f.calls
	f.calls++
	out := &lambda.ListFunctionsOutput{Functions: f.pages[idx]}
	if idx+1 < len(f.pages) {
		out.NextMarker = aws.String("next")
	}
	return out, nil
}

func TestListSSRFunctions_FiltersPrefixAndPaginates(t *testing.T) {
	appID1 := "3fa85f64-5717-4562-b3fc-2c963f66afa6"
	appID2 := "9c858901-8a57-4791-81fe-4c455b099bc9"

	fake := &fakeListFunctionsAPI{
		pages: [][]lambdatypes.FunctionConfiguration{
			{
				{FunctionName: aws.String("ms-apphost-" + appID1 + "-prod"), MemorySize: aws.Int32(512)},
				{FunctionName: aws.String("some-unrelated-lambda"), MemorySize: aws.Int32(1024)},
			},
			{
				{FunctionName: aws.String("ms-apphost-" + appID2 + "-prod"), MemorySize: aws.Int32(256)},
				{FunctionName: aws.String("ms-apphost-not-a-uuid-prod"), MemorySize: aws.Int32(128)},
			},
		},
	}
	c := &Client{lambdaAPI: fake}

	got, err := c.ListSSRFunctions(context.Background())
	if err != nil {
		t.Fatalf("ListSSRFunctions() error = %v", err)
	}
	if fake.calls != 2 {
		t.Errorf("calls = %d, want 2 (paginated across NextMarker)", fake.calls)
	}
	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2 (unrelated + unparseable functions skipped)", len(got))
	}
	if got[0].AppID.String() != appID1 || got[0].MemoryMB != 512 {
		t.Errorf("got[0] = %+v, want app %s / 512MB", got[0], appID1)
	}
	if got[1].AppID.String() != appID2 || got[1].MemoryMB != 256 {
		t.Errorf("got[1] = %+v, want app %s / 256MB", got[1], appID2)
	}
}
