package platformfunded_test

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/mirrorstack-ai/billing-engine/internal/account/platformfunded"
	"github.com/mirrorstack-ai/billing-engine/internal/account/platformfunded/pftest"
)

func TestParse_MembershipIsExactlyTheConfiguredList(t *testing.T) {
	a, b, outsider := uuid.New(), uuid.New(), uuid.New()
	set, err := platformfunded.Parse(" " + a.String() + " ,\t" + b.String() + ",," + a.String())
	require.NoError(t, err)
	require.Equal(t, 2, set.Len(), "blank and duplicate entries collapse")
	require.True(t, set.Funded(a))
	require.True(t, set.Funded(b))
	require.False(t, set.Funded(outsider), "an org outside PORG never gets the attribute")
	require.False(t, set.Funded(uuid.Nil))
}

func TestEmptyConfigAndZeroSetFundNobody(t *testing.T) {
	var zero platformfunded.Set
	require.False(t, zero.Funded(uuid.New()))
	for _, raw := range []string{"", " , "} {
		set, err := platformfunded.Parse(raw)
		require.NoError(t, err, raw)
		require.Zero(t, set.Len(), raw)
	}
}

func TestParse_AnyBadEntryFailsClosedToTheEmptySet(t *testing.T) {
	good := uuid.New()
	var tooMany []string
	for i := 0; i <= platformfunded.MaxOrgs; i++ {
		tooMany = append(tooMany, uuid.NewString())
	}
	for name, raw := range map[string]string{
		"malformed":    good.String() + ",not-a-uuid",
		"nil uuid":     good.String() + "," + uuid.Nil.String(),
		"over the cap": strings.Join(tooMany, ","),
	} {
		set, err := platformfunded.Parse(raw)
		require.Error(t, err, name)
		require.Zero(t, set.Len(), "%s: the good entry must NOT survive a bad one", name)
		require.False(t, set.Funded(good), name)
	}
}

func TestFromProcessEnv_ReadsPlatformOrgIDsAndRejectsGarbageWithoutCrashing(t *testing.T) {
	org := uuid.New()
	t.Setenv(platformfunded.EnvOrgIDs, org.String())
	require.True(t, platformfunded.FromProcessEnv().Funded(org))
	t.Setenv(platformfunded.EnvOrgIDs, "garbage")
	require.Zero(t, platformfunded.FromProcessEnv().Len())
}

func TestAudit_WritesTheStableEventWithGateAndOrg(t *testing.T) {
	uses := pftest.Capture(t)
	org := uuid.New()
	platformfunded.Audit(context.Background(), platformfunded.GateRegisterApp, org)
	require.Equal(t, []pftest.Use{{Gate: "register_app", OrgID: org.String()}}, uses())
}

// "No setter route, no operator, no database write" as a property of the
// package: its exported surface is the read side plus the audit line, and it
// imports nothing that reaches a database or the network.
func TestPackageHasNoSetterAndNoDatabase(t *testing.T) {
	pkgs, err := parser.ParseDir(token.NewFileSet(), ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	require.NoError(t, err)
	var exported []string
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			for _, imp := range file.Imports {
				for _, banned := range []string{"database/sql", "net/http", "jackc/pgx", "account/db", "shared/stripe"} {
					require.NotContains(t, imp.Path.Value, banned, name)
				}
			}
			for _, decl := range file.Decls {
				if d, ok := decl.(*ast.FuncDecl); ok && d.Name.IsExported() {
					if d.Recv != nil {
						exported = append(exported, "Set."+d.Name.Name)
					} else {
						exported = append(exported, d.Name.Name)
					}
				}
			}
		}
	}
	sort.Strings(exported)
	require.Equal(t, []string{"Audit", "FromProcessEnv", "Parse", "Set.Funded", "Set.Len"}, exported)
}
