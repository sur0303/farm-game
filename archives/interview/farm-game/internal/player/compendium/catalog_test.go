package compendium

import "testing"

func validCatalog() CatalogSnapshot {
	return CatalogSnapshot{
		CatalogVersion: 1,
		Entries: []PlantCatalogEntry{
			normalEntry(1),
			activityEntry(2, "spring_fest", ActivityStateActive, true),
			activityEntry(3, "winter_fest", ActivityStateArchived, true),
		},
		Archives: []ActivityArchiveDefinition{{
			ActivityID:           "winter_fest",
			ArchiveVersion:       1,
			CropDefinitionIDs:    []uint32{3},
			ArchiveTitleSnapshot: "冬日祭",
			ArchiveOrder:         1,
			EndAtMs:              1700000000000,
		}},
	}
}

func TestValidateCatalog_AcceptsValid(t *testing.T) {
	if err := ValidateCatalog(validCatalog(), testLimits()); err != nil {
		t.Fatalf("valid catalog rejected: %v", err)
	}
}

func TestValidateCatalog_RejectMatrix(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*CatalogSnapshot)
		code ErrorCode
	}{
		{"zero catalog version", func(c *CatalogSnapshot) { c.CatalogVersion = 0 }, ErrInvalidArgument},
		{"zero crop id", func(c *CatalogSnapshot) { c.Entries[0].CropDefinitionID = 0 }, ErrInvalidArgument},
		{"duplicate crop id", func(c *CatalogSnapshot) {
			c.Entries[1] = normalEntry(1)
		}, ErrInvalidArgument},
		{"normal entry with activity fields", func(c *CatalogSnapshot) {
			c.Entries[0].ActivityID = "ghost"
		}, ErrInvalidArgument},
		{"activity entry without id", func(c *CatalogSnapshot) {
			c.Entries[1].ActivityID = ""
		}, ErrInvalidArgument},
		{"activity entry with bad state", func(c *CatalogSnapshot) {
			c.Entries[1].ActivityState = ActivityStateUnspecified
		}, ErrInvalidArgument},
		{"archived entry without archive def", func(c *CatalogSnapshot) {
			c.Archives = nil
		}, ErrInvalidArgument},
		{"archive misses archived entry", func(c *CatalogSnapshot) {
			c.Archives[0].CropDefinitionIDs = nil
		}, ErrInvalidArgument},
		{"archive references unknown crop", func(c *CatalogSnapshot) {
			c.Archives[0].CropDefinitionIDs = []uint32{3, 999}
		}, ErrInvalidArgument},
		{"archive lists crop twice", func(c *CatalogSnapshot) {
			c.Archives[0].CropDefinitionIDs = []uint32{3, 3}
		}, ErrInvalidArgument},
		{"duplicate archive id", func(c *CatalogSnapshot) {
			c.Archives = append(c.Archives, c.Archives[0])
		}, ErrInvalidArgument},
		{"duplicate archive order breaks cursor totality", func(c *CatalogSnapshot) {
			dup := c.Archives[0]
			dup.ActivityID = "other_fest"
			c.Archives = append(c.Archives, dup)
			c.Entries = append(c.Entries, activityEntry(4, "other_fest", ActivityStateArchived, false))
			dup.CropDefinitionIDs = []uint32{4}
			c.Archives[1] = dup
		}, ErrInvalidArgument},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validCatalog()
			tc.mut(&c)
			err := ValidateCatalog(c, testLimits())
			if err == nil || err.Code != tc.code {
				t.Fatalf("got %v, want code %s", err, tc.code)
			}
		})
	}
}

func TestValidateCatalog_CapacityGates(t *testing.T) {
	t.Run("too many entries", func(t *testing.T) {
		limits := testLimits()
		limits.MaxCatalogEntries = 2
		if err := ValidateCatalog(validCatalog(), limits); err == nil || err.Code != ErrWorkCapacityExceeded {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("too many archives", func(t *testing.T) {
		limits := testLimits()
		limits.MaxActivityArchives = 0
		if err := ValidateCatalog(validCatalog(), limits); err == nil {
			t.Fatal("zero archive bound must fail validation itself")
		}
	})
	t.Run("collectible entries bounded by unlock cap", func(t *testing.T) {
		limits := testLimits()
		limits.MaxUnlockedEntries = 2
		if err := ValidateCatalog(validCatalog(), limits); err == nil || err.Code != ErrWorkCapacityExceeded {
			t.Fatalf("got %v", err)
		}
	})
}

func TestLimitProfile_ZeroBoundsRejected(t *testing.T) {
	if err := (LimitProfile{}).Validate(); err == nil {
		t.Fatal("empty profile must be rejected; zero bounds would disable fences")
	}
}
