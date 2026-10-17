package authoritystore

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"testing"
)

const sealedCommitStampPackage = "farmgame/internal/storage/internal/commitstamp"

// TestCommitStampIssuanceHasOnlyTrustedAdapterCallers is a static architecture
// fence. The nested internal package provides the compiler boundary; this
// allowlist additionally pins every capability reference to the two definitive
// first-COMMITTED adapter paths and the read-only authoritystore verifier.
func TestCommitStampIssuanceHasOnlyTrustedAdapterCallers(t *testing.T) {
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve test source path")
	}
	root := filepath.Clean(filepath.Join(filepath.Dir(currentFile), "..", "..", ".."))
	allowed := map[string]map[string]int{
		"internal/storage/authoritystore/commitstamp.go": {
			"<package>.Stamp":            1,
			"ExplicitCommit.VerifyExact": 1,
		},
		"internal/storage/authoritystore/memory.go": {
			"commit.CapturedRequest":     1,
			"commit.CaptureExactRequest": 1,
			"commit.IssueFirstCommitted": 1,
		},
		"internal/storage/redis/authority/store.go": {
			"commit.CapturedRequest":                1,
			"commit.CaptureExactRequest":            1,
			"decodeCommitReply.CapturedRequest":     1,
			"decodeCommitReply.IssueFirstCommitted": 1,
		},
	}
	seen := make(map[string]map[string]int, len(allowed))
	var violations []string
	fileSet := token.NewFileSet()
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "third_party":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		file, err := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		aliases := map[string]struct{}{}
		for _, imported := range file.Imports {
			importPath, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				return err
			}
			if importPath != sealedCommitStampPackage {
				continue
			}
			if _, ok := allowed[rel]; !ok {
				violations = append(violations, rel+": imports sealed commit-stamp capability")
			}
			alias := "commitstamp"
			if imported.Name != nil {
				alias = imported.Name.Name
			}
			if alias == "." || alias == "_" {
				violations = append(violations, rel+": dot/blank import of sealed commit-stamp capability")
				continue
			}
			aliases[alias] = struct{}{}
		}
		if len(aliases) == 0 {
			return nil
		}
		file, err = parser.ParseFile(fileSet, path, nil, 0)
		if err != nil {
			return err
		}
		inspectSelectors := func(node ast.Node, declaration string) {
			ast.Inspect(node, func(node ast.Node) bool {
				selector, ok := node.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				identifier, ok := selector.X.(*ast.Ident)
				if !ok {
					return true
				}
				if _, ok := aliases[identifier.Name]; !ok {
					return true
				}
				reference := declaration + "." + selector.Sel.Name
				want, trustedFile := allowed[rel]
				if !trustedFile {
					return true
				}
				if _, trustedReference := want[reference]; !trustedReference {
					violations = append(violations, rel+": unexpected sealed capability reference "+reference)
					return true
				}
				if seen[rel] == nil {
					seen[rel] = map[string]int{}
				}
				seen[rel][reference]++
				return true
			})
		}
		for _, declaration := range file.Decls {
			switch typed := declaration.(type) {
			case *ast.FuncDecl:
				inspectSelectors(typed, typed.Name.Name)
			default:
				inspectSelectors(declaration, "<package>")
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for file, references := range allowed {
		for reference, count := range references {
			if seen[file][reference] != count {
				violations = append(violations, file+": sealed reference "+reference+" count mismatch")
			}
		}
	}
	if len(violations) > 0 {
		sort.Strings(violations)
		for _, violation := range violations {
			t.Error(violation)
		}
	}
}

// TestCommitStampPublicAPIIsSealed proves CommitResult exposes an interface
// whose sole implementation method belongs to the nested adapter package. A
// caller can assemble a nil field, but cannot define a non-nil implementation
// or access an issuance/finalization method through authoritystore.
func TestCommitStampPublicAPIIsSealed(t *testing.T) {
	stampType := reflect.TypeOf((*CommitStamp)(nil)).Elem()
	if stampType.Kind() != reflect.Interface {
		t.Fatalf("CommitStamp kind=%s, want sealed interface", stampType.Kind())
	}
	if stampType.NumMethod() != 1 {
		t.Fatalf("CommitStamp methods=%d, want one package-private seal", stampType.NumMethod())
	}
	seal := stampType.Method(0)
	if seal.Name != "seal" || seal.PkgPath != sealedCommitStampPackage {
		t.Fatalf("CommitStamp seal method=%s package=%q", seal.Name, seal.PkgPath)
	}
	field, ok := reflect.TypeOf(CommitResult{}).FieldByName("CommitStamp")
	if !ok || field.Type != stampType || field.Type.Kind() != reflect.Interface {
		t.Fatalf("CommitResult.CommitStamp type=%v", field.Type)
	}
}
