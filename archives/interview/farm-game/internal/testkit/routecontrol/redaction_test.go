package routecontrol

import (
	"strings"
	"testing"
)

func TestRedactorRemovesSecretsAndURLCredentials(t *testing.T) {
	redactor := NewRedactor("super-secret")
	input := "Authorization: Bearer super-secret password=super-secret token=super-secret api-key=super-secret redis://user:developer_004@example.invalid:6379/0?api_key=super-secret 198.18.0.205:6379"
	output := redactor.Redact(input)
	for _, forbidden := range []string{"super-secret", "real.example", "198.18.0.205"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("output contains %q: %s", forbidden, output)
		}
	}
	for _, expected := range []string{"password=<redacted>", "token=<redacted>", "api-key=<redacted>", "<redacted-host>"} {
		if !strings.Contains(output, expected) {
			t.Fatalf("output missing %q: %s", expected, output)
		}
	}
	if !strings.Contains(output, "<redacted>") {
		t.Fatalf("output was not redacted: %s", output)
	}
}

func TestRedactorCoversJSONQuotedKeysHeadersAndSchemelessEndpoints(t *testing.T) {
	redactor := NewRedactor("json-password", "header-token", "endpoint-token")
	input := `{"password":"REPLACE_WITH_LOCAL_SECRET","token":"json-password","api-key":"json-password","safe":"keep"} X-API-Key: header-token endpoint=private.example:6379/path?region=us&token=endpoint-token#observe`
	output := redactor.Redact(input)
	for _, forbidden := range []string{"json-password", "header-token", "endpoint-token", "private.example"} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("output contains %q: %s", forbidden, output)
		}
	}
	for _, want := range []string{`"password":"<redacted>"`, `"token":"<redacted>"`, `"api-key":"<redacted>"`, "X-API-Key: <redacted>", "region=us", "#observe", "<redacted-host>:6379"} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q: %s", want, output)
		}
	}
}

func TestRedactorPreservesSchemelessEndpointWithoutPortAndIsIdempotent(t *testing.T) {
	input := "endpoint=private.example/path?region=us&token=secret#observe"
	redactor := NewRedactor("secret")
	once := redactor.Redact(input)
	twice := redactor.Redact(once)
	if once != twice {
		t.Fatalf("redaction was not idempotent: once=%q twice=%q", once, twice)
	}
	for _, want := range []string{"<redacted-host>/path", "region=us", "#observe", "token=<redacted>"} {
		if !strings.Contains(once, want) {
			t.Fatalf("redacted endpoint missing %q: %s", want, once)
		}
	}
}

func TestRedactorCoversAuthorizationBearerFormsAndURLFragmentQueries(t *testing.T) {
	input := `Authorization=Bearer bearer-secret Authorization: "Bearer quoted-secret" "Authorization":"Bearer json-secret" authorization='Bearer single-secret' https://private.example/run?keep=query&token=query-secret#section?keep=fragment&token=fragment-secret`
	output := NewRedactor().Redact(input)
	for _, forbidden := range []string{
		"Bearer bearer-secret",
		"Bearer quoted-secret",
		"Bearer json-secret",
		"Bearer single-secret",
		"query-secret",
		"fragment-secret",
		"private.example",
	} {
		if strings.Contains(output, forbidden) {
			t.Fatalf("output contains sensitive value %q: %s", forbidden, output)
		}
	}
	for _, want := range []string{
		"Authorization=Bearer <redacted>",
		`Authorization: "Bearer <redacted>"`,
		`"Authorization":"Bearer <redacted>"`,
		"authorization='Bearer <redacted>'",
		"keep=query",
		"keep=fragment",
		"#section?keep=fragment",
		"token=<redacted>",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q: %s", want, output)
		}
	}
}
