package fieldv2

import "testing"

func TestParseConversionPolicyDefaultOffAndStrict(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
	}{
		{name: "absent default"},
		{name: "qualified player", env: map[string]string{PlayerConversionEnabledEnv: "true", PlayerCohortPermyriadEnv: "125"}},
		{name: "disabled sticky cohort", env: map[string]string{FarmConversionEnabledEnv: "false", FarmCohortPermyriadEnv: "10000"}},
		{name: "empty boolean", env: map[string]string{PlayerConversionEnabledEnv: ""}, wantErr: true},
		{name: "alternate boolean", env: map[string]string{PlayerConversionEnabledEnv: "1"}, wantErr: true},
		{name: "enabled missing cohort", env: map[string]string{FarmConversionEnabledEnv: "true"}, wantErr: true},
		{name: "enabled zero cohort", env: map[string]string{FarmConversionEnabledEnv: "true", FarmCohortPermyriadEnv: "0"}, wantErr: true},
		{name: "empty cohort", env: map[string]string{FarmCohortPermyriadEnv: ""}, wantErr: true},
		{name: "cohort whitespace", env: map[string]string{FarmCohortPermyriadEnv: " 1"}, wantErr: true},
		{name: "cohort overflow", env: map[string]string{PlayerCohortPermyriadEnv: "10001"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy, err := ParseConversionPolicy(func(name string) (string, bool) {
				value, ok := test.env[name]
				return value, ok
			})
			if (err != nil) != test.wantErr {
				t.Fatalf("policy=%+v err=%v", policy, err)
			}
			if test.name == "absent default" && policy != DefaultConversionPolicy() {
				t.Fatalf("default policy=%+v", policy)
			}
		})
	}
}

func TestProductionConversionPolicyAlwaysFull(t *testing.T) {
	absent, err := ProductionConversionPolicy(func(string) (string, bool) { return "", false })
	if err != nil || absent != FullConversionPolicy() {
		t.Fatalf("absent=%+v err=%v", absent, err)
	}
	off, err := ProductionConversionPolicy(func(name string) (string, bool) {
		switch name {
		case PlayerConversionEnabledEnv, FarmConversionEnabledEnv:
			return "false", true
		default:
			return "", false
		}
	})
	if err != nil || off != FullConversionPolicy() {
		t.Fatalf("explicit off still uses sparse v2 writes off=%+v err=%v", off, err)
	}
}
