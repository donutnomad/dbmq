package dbmq

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMachineID(t *testing.T) {
	t.Run("returns hostname:mac format", func(t *testing.T) {
		machineID := GetMachineID()

		// Should contain at least one colon (between hostname and MAC)
		assert.Contains(t, machineID, ":")

		// Should have format hostname:mac where mac is either "unknown" or contains colons
		parts := strings.SplitN(machineID, ":", 2)
		require.Len(t, parts, 2, "machineID should have at least one colon")

		hostname := parts[0]
		mac := parts[1]

		// Hostname should not be empty
		assert.NotEmpty(t, hostname, "hostname should not be empty")

		// MAC should be either "unknown" or a valid MAC address format (aa:bb:cc:dd:ee:ff)
		if mac != "unknown" {
			// MAC address format: aa:bb:cc:dd:ee:ff (contains 5 colons)
			colonCount := strings.Count(mac, ":")
			assert.Equal(t, 5, colonCount, "MAC address should contain 5 colons")
		}
	})
}

func TestGenerateConsumerID_WithClientID(t *testing.T) {
	t.Run("uses provided clientID directly", func(t *testing.T) {
		clientID := "my-custom-consumer-id"
		consumerID := GenerateConsumerID(clientID)

		assert.Equal(t, clientID, consumerID)
	})

	t.Run("preserves complex clientID", func(t *testing.T) {
		clientID := "app-server-01:consumer:partition-0"
		consumerID := GenerateConsumerID(clientID)

		assert.Equal(t, clientID, consumerID)
	})
}

func TestGenerateConsumerID_WithoutClientID(t *testing.T) {
	t.Run("generates hostname:mac:uuid format", func(t *testing.T) {
		consumerID := GenerateConsumerID("")

		// Should contain multiple colons (hostname + MAC + UUID)
		colonCount := strings.Count(consumerID, ":")
		// hostname:MAC(5 colons):UUID = at least 7 colons, or hostname:unknown:UUID = 2 colons
		assert.GreaterOrEqual(t, colonCount, 2, "consumerID should have at least 2 colons")

		// Should contain the machine ID as prefix
		machineID := GetMachineID()
		assert.True(t, strings.HasPrefix(consumerID, machineID+":"),
			"consumerID should start with machineID prefix")
	})

	t.Run("generates unique IDs on each call", func(t *testing.T) {
		id1 := GenerateConsumerID("")
		id2 := GenerateConsumerID("")
		id3 := GenerateConsumerID("")

		assert.NotEqual(t, id1, id2, "generated IDs should be unique")
		assert.NotEqual(t, id2, id3, "generated IDs should be unique")
		assert.NotEqual(t, id1, id3, "generated IDs should be unique")
	})

	t.Run("contains valid UUID at the end", func(t *testing.T) {
		consumerID := GenerateConsumerID("")

		// Extract the UUID part (last 36 characters for standard UUID format)
		// UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars)
		if len(consumerID) >= 36 {
			uuidPart := consumerID[len(consumerID)-36:]
			// UUID should contain 4 hyphens
			hyphenCount := strings.Count(uuidPart, "-")
			assert.Equal(t, 4, hyphenCount, "UUID should contain 4 hyphens")
		}
	})
}

func TestGetConsumerIDPrefix(t *testing.T) {
	t.Run("extracts prefix from standard consumerID", func(t *testing.T) {
		// Standard format: hostname:aa:bb:cc:dd:ee:ff:uuid
		consumerID := "myhost:aa:bb:cc:dd:ee:ff:12345678-1234-1234-1234-123456789abc"
		prefix := GetConsumerIDPrefix(consumerID)

		// Should return hostname:MAC (first 7 colon-separated parts)
		expected := "myhost:aa:bb:cc:dd:ee:ff"
		assert.Equal(t, expected, prefix)
	})

	t.Run("extracts prefix from generated consumerID", func(t *testing.T) {
		// Generate a real consumerID
		consumerID := GenerateConsumerID("")
		prefix := GetConsumerIDPrefix(consumerID)

		// Prefix should be the machine ID
		machineID := GetMachineID()
		assert.Equal(t, machineID, prefix)
	})

	t.Run("returns full string if less than 7 colons", func(t *testing.T) {
		// If the consumerID doesn't have enough colons, return the full string
		shortID := "simple-id:part2"
		prefix := GetConsumerIDPrefix(shortID)

		assert.Equal(t, shortID, prefix)
	})

	t.Run("handles consumerID with unknown MAC", func(t *testing.T) {
		// Format with unknown MAC: hostname:unknown:uuid
		consumerID := "myhost:unknown:12345678-1234-1234-1234-123456789abc"
		prefix := GetConsumerIDPrefix(consumerID)

		// With only 2 colons, should return the full string
		assert.Equal(t, consumerID, prefix)
	})
}

func TestMatchConsumerIDPrefix(t *testing.T) {
	t.Run("matches when prefix is exact", func(t *testing.T) {
		consumerID := "myhost:aa:bb:cc:dd:ee:ff:uuid-123"
		prefix := "myhost:aa:bb:cc:dd:ee:ff"

		assert.True(t, MatchConsumerIDPrefix(consumerID, prefix))
	})

	t.Run("matches when prefix is shorter", func(t *testing.T) {
		consumerID := "myhost:aa:bb:cc:dd:ee:ff:uuid-123"
		prefix := "myhost"

		assert.True(t, MatchConsumerIDPrefix(consumerID, prefix))
	})

	t.Run("does not match when prefix differs", func(t *testing.T) {
		consumerID := "myhost:aa:bb:cc:dd:ee:ff:uuid-123"
		prefix := "otherhost:aa:bb:cc:dd:ee:ff"

		assert.False(t, MatchConsumerIDPrefix(consumerID, prefix))
	})

	t.Run("does not match when prefix is longer than consumerID", func(t *testing.T) {
		consumerID := "myhost"
		prefix := "myhost:aa:bb:cc:dd:ee:ff"

		assert.False(t, MatchConsumerIDPrefix(consumerID, prefix))
	})

	t.Run("matches empty prefix with any consumerID", func(t *testing.T) {
		consumerID := "myhost:aa:bb:cc:dd:ee:ff:uuid-123"
		prefix := ""

		assert.True(t, MatchConsumerIDPrefix(consumerID, prefix))
	})

	t.Run("matches same machine IDs from generated consumerIDs", func(t *testing.T) {
		// Generate two consumer IDs on the same machine
		id1 := GenerateConsumerID("")
		id2 := GenerateConsumerID("")

		// Extract their prefixes (should be the same machine ID)
		prefix1 := GetConsumerIDPrefix(id1)
		prefix2 := GetConsumerIDPrefix(id2)

		// Both should match each other's prefix
		assert.True(t, MatchConsumerIDPrefix(id1, prefix2))
		assert.True(t, MatchConsumerIDPrefix(id2, prefix1))

		// Prefixes should be equal
		assert.Equal(t, prefix1, prefix2)
	})
}
