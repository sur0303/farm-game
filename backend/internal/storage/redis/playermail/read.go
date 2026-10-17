package playermail

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"farmgame/internal/player/mail"
)

// maxSortMemberBytes bounds one SortMemberV1 (8B inverted time + 0x00 +
// "sid1_" + 43B base64url(SHA-256) = 57B), padded.
const maxSortMemberBytes = 64

// ListPage implements mail.MailReadRepository with the bounded overscan of
// 35A §3: ZRANGEBYLEX candidates after the exclusive cursor, explicit-field
// HMGET, time/read/delete filtering, at most PageSize results, and hard
// PageScanItems/PageScanBytes caps that permit a short page.
//
// FIX-4: every issued HMGET is byte-bounded up front. Each batch's
// candidate count is capped by the remaining byte budget divided by the
// worst-case per-candidate encoded size (MaxMailEntryBytes +
// MaxMailStateBytes + SortMember), so a single Redis reply can never exceed
// PageScanBytes before accounting runs.
func (s *Store) ListPage(ctx context.Context, recipientPlayerID uint64, cursor string, nowMs int64) (mail.Page, error) {
	mk, err := s.mailbox(recipientPlayerID)
	if err != nil {
		return mail.Page{}, err
	}
	min := "-"
	if cursor != "" {
		member, err := mail.DecodeCursor(cursor)
		if err != nil {
			return mail.Page{}, err
		}
		min = "(" + string(member)
	}
	maxCandidateBytes := s.limits.MaxMailEntryBytes + s.limits.MaxMailStateBytes + maxSortMemberBytes
	page := mail.Page{Items: make([]mail.ListItem, 0, s.limits.PageSize)}
	scannedItems := 0
	scannedBytes := 0
	lastScanned := ""
	exhausted := false

	for len(page.Items) < s.limits.PageSize && !exhausted {
		remainingItems := s.limits.PageScanItems - scannedItems
		remainingBytes := s.limits.PageScanBytes - scannedBytes
		if remainingItems <= 0 || remainingBytes < maxCandidateBytes {
			// Cannot guarantee even one more candidate inside the byte
			// budget: stop with a short page (35 Q35-05).
			break
		}
		batch := s.limits.PageSize * 2
		if batch > remainingItems {
			batch = remainingItems
		}
		if maxByBytes := remainingBytes / maxCandidateBytes; batch > maxByBytes {
			batch = maxByBytes
		}
		if batch < 1 {
			break
		}
		members, err := s.client.ZRangeByLex(ctx, mk.index.String(), &redis.ZRangeBy{
			Min: min, Max: "+", Offset: 0, Count: int64(batch),
		}).Result()
		if err != nil {
			return mail.Page{}, err
		}
		if len(members) == 0 {
			exhausted = true
			break
		}
		if len(members) < batch {
			exhausted = true
		}
		min = "(" + members[len(members)-1]

		fields := make([]string, 0, len(members)*2)
		ids := make([]string, 0, len(members))
		for _, member := range members {
			mailID, err := mail.MailIDFromSortMember([]byte(member))
			if err != nil {
				return mail.Page{}, fmt.Errorf("%w: index member: %v", mail.ErrContractViolation, err)
			}
			entryField, err := s.fields.EntryField(mailID)
			if err != nil {
				return mail.Page{}, err
			}
			stateField, err := s.fields.StateField(mailID)
			if err != nil {
				return mail.Page{}, err
			}
			fields = append(fields, entryField, stateField)
			ids = append(ids, mailID)
		}
		vals, err := s.client.HMGet(ctx, mk.box.String(), fields...).Result()
		if err != nil {
			return mail.Page{}, err
		}
		for i, member := range members {
			if len(page.Items) >= s.limits.PageSize {
				break
			}
			if scannedItems >= s.limits.PageScanItems || scannedBytes >= s.limits.PageScanBytes {
				break
			}
			scannedItems++
			scannedBytes += len(member)
			lastScanned = member
			entryVal, stateVal := vals[2*i], vals[2*i+1]
			if entryVal == nil || stateVal == nil {
				continue // purged between index read and HMGET
			}
			entryBlob, ok := entryVal.(string)
			if !ok {
				return mail.Page{}, fmt.Errorf("%w: entry blob type", mail.ErrContractViolation)
			}
			stateBlob, ok := stateVal.(string)
			if !ok {
				return mail.Page{}, fmt.Errorf("%w: state blob type", mail.ErrContractViolation)
			}
			scannedBytes += len(entryBlob) + len(stateBlob)
			entry, err := mail.DecodeMailEntry([]byte(entryBlob))
			if err != nil {
				return mail.Page{}, err
			}
			state, err := mail.DecodeMailUserState([]byte(stateBlob))
			if err != nil {
				return mail.Page{}, err
			}
			if !state.Visible(entry, nowMs) {
				continue
			}
			page.Items = append(page.Items, mail.ListItem{
				MailID:            ids[i],
				Title:             entry.TitleSnapshot,
				CreatedAtMs:       entry.CreatedAtMs,
				ExpireAtMs:        entry.ExpireAtMs,
				Read:              state.ReadAtMs > 0,
				HasAttachment:     entry.HasAttachment(),
				ClaimedProjection: state.ClaimedProjection,
			})
		}
	}

	cursorMember := ""
	if len(page.Items) == s.limits.PageSize {
		// A full page continues after its last returned item. Probe one
		// member further so the terminal page emits no cursor, matching the
		// reference fake exactly.
		last := page.Items[len(page.Items)-1]
		member, err := mail.EncodeSortMember(last.CreatedAtMs, last.MailID)
		if err != nil {
			return mail.Page{}, err
		}
		probe, err := s.client.ZRangeByLex(ctx, mk.index.String(), &redis.ZRangeBy{
			Min: "(" + string(member), Max: "+", Offset: 0, Count: 1,
		}).Result()
		if err != nil {
			return mail.Page{}, err
		}
		if len(probe) > 0 {
			cursorMember = string(member)
		}
	} else if !exhausted {
		// A short page caused by the scan caps resumes after the last
		// examined candidate (35 Q35-05); a short page at the index tail
		// is simply the end.
		cursorMember = lastScanned
	}
	if cursorMember == "" {
		return page, nil
	}
	encoded, err := mail.EncodeCursor([]byte(cursorMember))
	if err != nil {
		return mail.Page{}, err
	}
	page.NextCursor = encoded
	return page, nil
}
