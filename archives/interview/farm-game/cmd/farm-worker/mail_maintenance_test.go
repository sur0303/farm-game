package main

import (
	"testing"
	"time"
)

func TestMailMaintenanceEnvironmentParsingFailsClosed(t *testing.T) {
	t.Setenv("MAIL_MAINTENANCE_TEST_INT", "not-an-int")
	if _, err := mailMaintenanceInt("MAIL_MAINTENANCE_TEST_INT", 7, 0); err == nil {
		t.Fatal("malformed integer must fail closed")
	}
	t.Setenv("MAIL_MAINTENANCE_TEST_INT", "-1")
	if _, err := mailMaintenanceInt("MAIL_MAINTENANCE_TEST_INT", 7, 0); err == nil {
		t.Fatal("negative worker index must fail closed")
	}
	t.Setenv("MAIL_MAINTENANCE_TEST_INT", "0")
	if value, err := mailMaintenanceInt("MAIL_MAINTENANCE_TEST_INT", 7, 0); err != nil || value != 0 {
		t.Fatalf("zero worker index value=%d err=%v", value, err)
	}

	t.Setenv("MAIL_MAINTENANCE_TEST_DURATION", "forever")
	if _, err := mailMaintenanceEnvDuration("MAIL_MAINTENANCE_TEST_DURATION", time.Second); err == nil {
		t.Fatal("malformed duration must fail closed")
	}
}
