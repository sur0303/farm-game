package routecontrol

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	authorizationPattern    = regexp.MustCompile(`(?i)(\b(?:authorization|proxy-authorization)\s*["']?\s*[:=]\s*)(["']?)(Bearer|Basic|Digest)\s+([^"'\s,;}\]]+)(["']?)`)
	quotedKeyValuePattern   = regexp.MustCompile(`(?i)(^|[\s"'?&;,{\[(])((?:password|passwd|token|secret|auth|api[-_]?key|x-api-key|access[-_]?token|refresh[-_]?token|client[-_]?secret))(["']?\s*[:=]\s*)("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')`)
	unquotedKeyValuePattern = regexp.MustCompile(`(?i)(^|[\s"'?&;,{\[(])((?:authorization|proxy-authorization|password|passwd|token|secret|auth|api[-_]?key|x-api-key|access[-_]?token|refresh[-_]?token|client[-_]?secret))(["']?\s*[:=]\s*)([^\s&;,}\]\)"'#]+)`)
	endpointPattern         = regexp.MustCompile(`(?i)\b[a-z][a-z0-9+.-]*://[^\s"']+`)
	labeledEndpointPattern  = regexp.MustCompile(`(?i)(\b(?:endpoint|host|address|addr|url)\s*[:=]\s*)([^\s"']+)`)
	hostPortPattern         = regexp.MustCompile(`(?i)\b(?:localhost|(?:[a-z0-9-]+\.)+[a-z]{2,}|(?:\d{1,3}\.){3}\d{1,3}):\d{1,5}\b`)
)

// Redactor removes caller-provided secret values and common URL credential
// forms before free text is written to evidence.
type Redactor struct {
	secrets []string
}

// NewRedactor returns a redactor. Empty secret values are ignored.
func NewRedactor(secrets ...string) *Redactor {
	filtered := make([]string, 0, len(secrets))
	for _, secret := range secrets {
		if strings.TrimSpace(secret) != "" {
			filtered = append(filtered, secret)
		}
	}
	return &Redactor{secrets: filtered}
}

// Redact returns text safe for evidence output. It does not mutate input.
func (r *Redactor) Redact(value string) string {
	if r == nil {
		r = &Redactor{}
	}
	for _, secret := range r.secrets {
		value = strings.ReplaceAll(value, secret, "<redacted>")
	}
	value = authorizationPattern.ReplaceAllStringFunc(value, redactAuthorization)
	value = quotedKeyValuePattern.ReplaceAllStringFunc(value, redactQuotedKeyValue)
	value = unquotedKeyValuePattern.ReplaceAllStringFunc(value, redactUnquotedKeyValue)
	value = labeledEndpointPattern.ReplaceAllStringFunc(value, redactLabeledEndpoint)
	value = endpointPattern.ReplaceAllStringFunc(value, redactEndpoint)
	return hostPortPattern.ReplaceAllStringFunc(value, redactBareHostPort)
}

func redactAuthorization(value string) string {
	parts := authorizationPattern.FindStringSubmatch(value)
	if len(parts) != 6 {
		return value
	}
	closing := ""
	if parts[2] != "" && parts[2] == parts[5] {
		closing = parts[5]
	}
	return parts[1] + parts[2] + parts[3] + " <redacted>" + closing
}

func redactQuotedKeyValue(value string) string {
	parts := quotedKeyValuePattern.FindStringSubmatch(value)
	if len(parts) != 5 {
		return value
	}
	key := strings.ToLower(parts[2])
	inner := strings.Trim(parts[4], `"'`)
	if (key == "authorization" || key == "proxy-authorization") &&
		hasAuthScheme(inner) {
		// authorizationPattern already redacted the credential while retaining
		// the scheme and quotes.
		return value
	}
	quote := parts[4][0]
	return parts[1] + parts[2] + parts[3] + string(quote) + "<redacted>" + string(quote)
}

func redactUnquotedKeyValue(value string) string {
	parts := unquotedKeyValuePattern.FindStringSubmatch(value)
	if len(parts) != 5 {
		return value
	}
	if (strings.EqualFold(parts[2], "authorization") || strings.EqualFold(parts[2], "proxy-authorization")) &&
		hasAuthScheme(parts[4]) {
		return value
	}
	return parts[1] + parts[2] + parts[3] + "<redacted>"
}

func hasAuthScheme(value string) bool {
	fields := strings.Fields(value)
	if len(fields) == 0 {
		return false
	}
	switch strings.ToLower(fields[0]) {
	case "bearer", "basic", "digest":
		return true
	default:
		return false
	}
}

func redactLabeledEndpoint(value string) string {
	parts := labeledEndpointPattern.FindStringSubmatch(value)
	if len(parts) != 3 {
		return value
	}
	return parts[1] + redactEndpoint(parts[2])
}

func redactEndpoint(value string) string {
	suffix := ""
	for len(value) > 0 && strings.ContainsRune(".,;:!?)]}", rune(value[len(value)-1])) {
		suffix = string(value[len(value)-1]) + suffix
		value = value[:len(value)-1]
	}
	if value == "" {
		return "<redacted-endpoint>" + suffix
	}

	parsed, err := url.Parse(value)
	hasScheme := err == nil && parsed.Scheme != "" && parsed.Host != ""
	if !hasScheme {
		parsed, err = url.Parse("//" + value)
	}
	if err != nil || parsed.Host == "" {
		return "<redacted-endpoint>" + suffix
	}

	parsed.User = nil
	parsed.Host = redactedHost(parsed.Host, parsed.Port())
	query := parsed.Query()
	for key := range query {
		if sensitiveQueryKey(key) {
			query[key] = []string{"<redacted>"}
		}
	}
	parsed.RawQuery = query.Encode()
	parsed.Fragment = redactFragment(parsed.Fragment)
	parsed.RawFragment = ""
	result := parsed.String()
	if !hasScheme {
		result = strings.TrimPrefix(result, "//")
	}
	result = strings.ReplaceAll(result, "%3Credacted%3E", "<redacted>")
	return result + suffix
}

func redactFragment(fragment string) string {
	if fragment == "" {
		return fragment
	}
	fragment = authorizationPattern.ReplaceAllStringFunc(fragment, redactAuthorization)
	fragment = quotedKeyValuePattern.ReplaceAllStringFunc(fragment, redactQuotedKeyValue)
	fragment = unquotedKeyValuePattern.ReplaceAllStringFunc(fragment, redactUnquotedKeyValue)
	if queryStart := strings.IndexByte(fragment, '?'); queryStart >= 0 {
		return fragment[:queryStart+1] + redactQuery(fragment[queryStart+1:])
	}
	return redactQuery(fragment)
}

func redactQuery(raw string) string {
	if !strings.Contains(raw, "=") {
		return raw
	}
	values, err := url.ParseQuery(raw)
	if err == nil {
		for key := range values {
			if sensitiveQueryKey(key) {
				values[key] = []string{"<redacted>"}
			}
		}
		return strings.ReplaceAll(values.Encode(), "%3Credacted%3E", "<redacted>")
	}
	// Preserve malformed/non-query fragments while still covering explicit
	// sensitive key=value pairs.
	return unquotedKeyValuePattern.ReplaceAllStringFunc(raw, redactUnquotedKeyValue)
}

func redactBareHostPort(value string) string {
	_, port, ok := strings.Cut(value, ":")
	if !ok {
		return "<redacted-host>"
	}
	return "<redacted-host>:" + port
}

func redactedHost(host, port string) string {
	if strings.Contains(host, "<redacted-host>") {
		return host
	}
	if port == "" {
		return "<redacted-host>"
	}
	return "<redacted-host>:" + port
}

func sensitiveQueryKey(key string) bool {
	normalized := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", ""), "_", ""))
	switch normalized {
	case "authorization", "proxyauthorization", "password", "passwd", "token",
		"secret", "auth", "apikey", "xapikey", "accesstoken", "refreshtoken",
		"clientsecret":
		return true
	default:
		return false
	}
}
