package friendshipservice

import "testing"

func TestModeFromEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		present bool
		want    Mode
		wantErr bool
	}{
		{name: "absent defaults to frozen rollback", want: ModeLocalGate},
		{name: "local", value: "local_gate", present: true, want: ModeLocalGate},
		{name: "shadow normalized", value: " REMOTE_SHADOW ", present: true, want: ModeRemoteShadow},
		{name: "remote", value: "remote_player", present: true, want: ModeRemotePlayer},
		{name: "explicit blank fails closed", value: "", present: true, wantErr: true},
		{name: "unknown fails closed", value: "dual_write", present: true, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := ModeFromEnvironment(func(string) (string, bool) { return test.value, test.present })
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("ModeFromEnvironment()=(%q,%v), want (%q, err=%v)", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestModeFromEnvironmentRequiresLookup(t *testing.T) {
	if _, err := ModeFromEnvironment(nil); err == nil {
		t.Fatal("nil environment lookup must fail closed")
	}
}
