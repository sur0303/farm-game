package actor_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonpb "farmgame/api/gen/common"
	logicrpcpb "farmgame/api/gen/logicrpc"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/identity"
	playeractor "farmgame/internal/player/actor"
	playercompendium "farmgame/internal/player/compendium"
	playeringress "farmgame/internal/player/ingress"
	"farmgame/internal/player/mail"
	playerpet "farmgame/internal/player/pet"
	playertask "farmgame/internal/player/task"
	"farmgame/internal/storage/authoritystore"

	"google.golang.org/protobuf/proto"
)

func executeMailClaimForTest(t *testing.T, service *playeringress.Service, operationID string, mailID string) *logicrpcpb.BusinessCommandOutcome {
	t.Helper()
	payload, err := proto.Marshal(&publicpb.ClaimMailAttachmentRequest{MailId: mailID})
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: &commonpb.ClientCommandEnvelope{
			OperationId: operationID,
			RequestId:   "request-" + operationID,
			CommandType: commonpb.CommandType_CLAIM_MAIL_ATTACHMENT,
			Payload:     payload,
		},
		Trusted: &logicrpcpb.TrustedCommandContext{
			Session:                     &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "mail-claim-test"},
			RecoveryCompleteAtAdmission: true,
		},
		Dispatch: &logicrpcpb.DispatchContext{
			RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
			ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: 9},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return response.GetBusiness()
}

func TestSupportSystemsFirstLoginClosedLoop(t *testing.T) {
	provider, _ := ar1IngressProvider(t, "memory", 9)
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	service := playeringress.NewService(registry)
	nowMs := time.Now().UnixMilli()
	if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, func() int64 { return nowMs }); err != nil {
		t.Fatal(err)
	}
	catalog, err := playercompendium.BuildDefaultCatalogV1()
	if err != nil {
		t.Fatal(err)
	}
	compendiumProvider, err := playercompendium.NewStaticCatalogProvider(catalog, playercompendium.DefaultLimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetCompendiumCatalogProvider(compendiumProvider, playercompendium.DefaultLimitsV1()); err != nil {
		t.Fatal(err)
	}
	if err := service.SetPetCatalogProvider(playerpet.MustNewP0Repository()); err != nil {
		t.Fatal(err)
	}
	mailStore, err := mail.NewMemoryStore(func() int64 { return nowMs }, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetMailRepositories(mailStore, mailStore); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{PlayerId: 9, BootstrapId: "support-loop-player"}); err != nil {
		t.Fatal(err)
	}
	welcome, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID:   9,
		DeliveryID:          "WELCOME:9:1",
		SourceType:          mail.SourceTypeSystem,
		SourceID:            "WELCOME_V1",
		TitleSnapshot:       "欢迎来到经典农场",
		BodySnapshot:        "欢迎回来，领取这份种子开始经营你的农场吧！",
		Reward:              mail.RewardBundle{Items: []mail.ItemGrant{{ItemID: playeractor.ItemCarrotSeed, Count: 3}}},
		TemplateVersion:     "welcome-v1",
		RewardConfigVersion: "welcome-reward-v1",
		CreatedAtMs:         nowMs,
		VisibleAtMs:         nowMs,
		RetentionMs:         int64(30 * 24 * time.Hour / time.Millisecond),
	}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mailStore.InsertMailIfAbsent(context.Background(), welcome); err != nil {
		t.Fatal(err)
	}

	bootstrap := executePlayerCommandForTest(t, service, "support-bootstrap", commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{})
	if bootstrap.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("bootstrap=%+v", bootstrap)
	}
	var bootstrapResult publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(bootstrap.GetResultPayload(), &bootstrapResult); err != nil {
		t.Fatal(err)
	}
	if got := bootstrapResult.GetFeatureSummary().GetMail(); got.GetUnreadCount() != 1 || got.GetClaimableAttachmentCount() != 1 || !got.GetHasRedPoint() {
		t.Fatalf("mail red point from mailbox read=%+v", got)
	}
	if got := bootstrapResult.GetFeatureSummary().GetTasks(); got.GetClaimableCount() != 1 {
		t.Fatalf("login task summary=%+v", got)
	}

	mailList := executeMailServiceCommandForTest(t, service, "support-mail-list", commonpb.CommandType_GET_MAIL_LIST, &publicpb.GetMailListRequest{})
	var mailListResult publicpb.GetMailListResult
	if mailList.GetStatus() != commonpb.CommandStatus_SUCCEEDED || proto.Unmarshal(mailList.GetResultPayload(), &mailListResult) != nil ||
		len(mailListResult.GetMails()) != 1 || mailListResult.GetMails()[0].GetTitle() != "欢迎来到经典农场" ||
		!mailListResult.GetMails()[0].GetHasAttachment() {
		t.Fatalf("mail list=%+v result=%+v", mailList, &mailListResult)
	}

	taskList := executePlayerCommandForTest(t, service, "support-task-list", commonpb.CommandType_GET_TASK_LIST, &publicpb.GetTaskListRequest{})
	var taskListResult publicpb.GetTaskListResult
	if taskList.GetStatus() != commonpb.CommandStatus_SUCCEEDED || proto.Unmarshal(taskList.GetResultPayload(), &taskListResult) != nil ||
		len(taskListResult.GetDailyTasks()) != playertask.DailySlotCount || taskListResult.GetCurrentGrowth() == nil {
		t.Fatalf("task list=%+v result=%+v", taskList, &taskListResult)
	}

	petState := executePlayerCommandForTest(t, service, "support-pet-state", commonpb.CommandType_GET_PET_STATE, &publicpb.GetPetStateRequest{})
	var petStateResult publicpb.GetPetStateResult
	if petState.GetStatus() != commonpb.CommandStatus_SUCCEEDED || proto.Unmarshal(petState.GetResultPayload(), &petStateResult) != nil ||
		petStateResult.GetPage() == nil || len(petStateResult.GetPage().GetPets()) != 2 || petStateResult.GetPage().GetState().GetGuardEffective() {
		t.Fatalf("pet state=%+v result=%+v", petState, &petStateResult)
	}

	compendium := executePlayerCommandForTest(t, service, "support-compendium", commonpb.CommandType_GET_COMPENDIUM, &publicpb.GetCompendiumRequest{})
	var compendiumResult publicpb.GetCompendiumResult
	if compendium.GetStatus() != commonpb.CommandStatus_SUCCEEDED || proto.Unmarshal(compendium.GetResultPayload(), &compendiumResult) != nil ||
		compendiumResult.GetPage() == nil || len(compendiumResult.GetPage().GetCategories()) == 0 || compendiumResult.GetPage().GetHasNewUnlock() {
		t.Fatalf("compendium=%+v result=%+v", compendium, &compendiumResult)
	}
}

func executeMailServiceCommandForTest(t *testing.T, service *playeringress.Service, operationID string, commandType commonpb.CommandType, payloadMessage proto.Message) *logicrpcpb.BusinessCommandOutcome {
	t.Helper()
	payload, err := proto.Marshal(payloadMessage)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: &commonpb.ClientCommandEnvelope{
			OperationId: operationID,
			RequestId:   "request-" + operationID,
			CommandType: commandType,
			Payload:     payload,
		},
		Trusted: &logicrpcpb.TrustedCommandContext{
			Session:                     &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "mail-service-test"},
			RecoveryCompleteAtAdmission: true,
		},
		Dispatch: &logicrpcpb.DispatchContext{RouteMode: logicrpcpb.RouteMode_ROUTE_MODE_SERVICE_LB},
	})
	if err != nil {
		t.Fatal(err)
	}
	return response.GetBusiness()
}

func executePlayerCommandForTest(t *testing.T, service *playeringress.Service, operationID string, commandType commonpb.CommandType, payloadMessage proto.Message) *logicrpcpb.BusinessCommandOutcome {
	t.Helper()
	payload, err := proto.Marshal(payloadMessage)
	if err != nil {
		t.Fatal(err)
	}
	response, err := service.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
		Command: &commonpb.ClientCommandEnvelope{
			OperationId: operationID,
			RequestId:   "request-" + operationID,
			CommandType: commandType,
			Payload:     payload,
		},
		Trusted: &logicrpcpb.TrustedCommandContext{
			Session:                     &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "player-command-test"},
			RecoveryCompleteAtAdmission: true,
		},
		Dispatch: &logicrpcpb.DispatchContext{
			RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
			ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: 9},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return response.GetBusiness()
}

func buildMailClaimEntry(t *testing.T, nowMs int64) mail.MailEntry {
	t.Helper()
	entry, err := mail.BuildMailEntry(mail.DeliveryParams{
		RecipientPlayerID:   9,
		DeliveryID:          "claim-integration-1",
		SourceType:          mail.SourceTypeTest,
		SourceID:            "mail-claim-integration",
		TitleSnapshot:       "integration reward",
		BodySnapshot:        "claim once",
		Reward:              mail.RewardBundle{Coin: 12, Exp: 3, Items: []mail.ItemGrant{{ItemID: playeractor.ItemCarrotSeed, Count: 2}}},
		TemplateVersion:     "mail-template-v1",
		RewardConfigVersion: "mail-reward-v1",
		CreatedAtMs:         nowMs,
		VisibleAtMs:         nowMs,
	}, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func TestMailClaimAuthorityCommitIsIdempotentAndPersistsRecoveryOrder(t *testing.T) {
	provider, store := ar1IngressProvider(t, "redis", 9)
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	service := playeringress.NewService(registry)
	nowMs := time.Now().UnixMilli()
	mailStore, err := mail.NewMemoryStore(time.Now().UnixMilli, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetMailRepositories(mailStore, mailStore); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{PlayerId: 9, BootstrapId: "mail-claim-player"}); err != nil {
		t.Fatal(err)
	}
	entry := buildMailClaimEntry(t, nowMs)
	if result, err := mailStore.InsertMailIfAbsent(context.Background(), entry); err != nil || result.Status != mail.InsertInserted {
		t.Fatalf("insert mail result=%+v err=%v", result, err)
	}

	first := executeMailClaimForTest(t, service, "mail-claim-op-1", entry.MailID)
	if first.GetStatus() != commonpb.CommandStatus_SUCCEEDED || first.GetError() != nil {
		t.Fatalf("first claim=%+v", first)
	}
	var firstResult publicpb.ClaimMailAttachmentResult
	if err := proto.Unmarshal(first.GetResultPayload(), &firstResult); err != nil {
		t.Fatal(err)
	}
	if firstResult.GetPrivateState().GetCoin() != playeractor.DefaultInitialCoin+12 {
		t.Fatalf("claim result=%s", firstResult.String())
	}
	replayed := executeMailClaimForTest(t, service, "mail-claim-op-1", entry.MailID)
	if replayed.GetStatus() != commonpb.CommandStatus_SUCCEEDED || string(replayed.GetResultPayload()) != string(first.GetResultPayload()) {
		t.Fatalf("same operation replay=%+v", replayed)
	}
	reclaimed := executeMailClaimForTest(t, service, "mail-claim-op-2", entry.MailID)
	if reclaimed.GetStatus() != commonpb.CommandStatus_NO_CHANGE || reclaimed.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_MAIL_ALREADY_CLAIMED {
		t.Fatalf("second operation reclaim=%+v", reclaimed)
	}

	record, _, err := registry.Read(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetWallet().GetCoin() != playeractor.DefaultInitialCoin+12 || playeractor.ItemCount(record, playeractor.ItemCarrotSeed) != 2 ||
		len(record.GetMailClaimState().GetReceipts()) != 1 {
		t.Fatalf("persisted record=%s", record.String())
	}
	state, err := mail.ClaimStateFromPlayerRecord(record)
	if err != nil || state.ClaimStatus(entry.MailID) != mail.ClaimStatusClaimed {
		t.Fatalf("claim state=%+v err=%v", state, err)
	}
	_, projected, err := mailStore.GetEntry(context.Background(), 9, entry.MailID)
	if err != nil || !projected.ClaimedProjection {
		t.Fatalf("mail projection=%+v err=%v", projected, err)
	}

	reader, ok := store.(authoritystore.SourceWorkReader)
	if !ok {
		t.Fatalf("authority store does not expose SourceWorkReader")
	}
	orderID, err := identity.SourceOrderID(identity.WorkTypeMailClaimProjectionOrder, identity.SourceKindPlayerActor, "9", entry.MailID, 1)
	if err != nil {
		t.Fatal(err)
	}
	work, err := reader.ReadSourceWork(context.Background(), authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 9}, string(identity.WorkTypeMailClaimProjectionOrder), orderID)
	if err != nil || work.WorkID != orderID || work.State != "PENDING" {
		t.Fatalf("source recovery work=%+v err=%v", work, err)
	}

	// A fresh process must recover the exact durable result and must not grant
	// the attachment a second time.
	restarted := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(restarted.Stop)
	service2 := playeringress.NewService(restarted)
	if err := service2.SetMailRepositories(mailStore, mailStore); err != nil {
		t.Fatal(err)
	}
	afterRestart := executeMailClaimForTest(t, service2, "mail-claim-op-1", entry.MailID)
	if afterRestart.GetStatus() != commonpb.CommandStatus_SUCCEEDED || string(afterRestart.GetResultPayload()) != string(first.GetResultPayload()) {
		t.Fatalf("restart replay=%+v", afterRestart)
	}
	reloaded, _, err := restarted.Read(context.Background(), 9)
	if err != nil || reloaded.GetWallet().GetCoin() != playeractor.DefaultInitialCoin+12 || playeractor.ItemCount(reloaded, playeractor.ItemCarrotSeed) != 2 {
		t.Fatalf("reloaded record=%v err=%v", reloaded, err)
	}
}

func TestMailClaimProjectionFailureDoesNotRollbackAssets(t *testing.T) {
	provider, _ := ar1IngressProvider(t, "memory", 9)
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	service := playeringress.NewService(registry)
	nowMs := time.Now().UnixMilli()
	mailStore, err := mail.NewMemoryStore(time.Now().UnixMilli, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetMailRepositories(mailStore, mailStore); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{PlayerId: 9, BootstrapId: "mail-claim-fault"}); err != nil {
		t.Fatal(err)
	}
	entry := buildMailClaimEntry(t, nowMs)
	if _, err := mailStore.InsertMailIfAbsent(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	mailStore.InjectFault(mail.FaultBeforeApply)
	outcome := executeMailClaimForTest(t, service, "mail-claim-fault-op", entry.MailID)
	if outcome.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("asset authority must win over projection failure: %+v", outcome)
	}
	record, _, err := registry.Read(context.Background(), 9)
	if err != nil || record.GetWallet().GetCoin() != playeractor.DefaultInitialCoin+12 || len(record.GetMailClaimState().GetReceipts()) != 1 {
		t.Fatalf("authority claim record=%v err=%v", record, err)
	}
	_, state, err := mailStore.GetEntry(context.Background(), 9, entry.MailID)
	if err != nil || state.ClaimedProjection {
		t.Fatalf("projection should remain pending: state=%+v err=%v", state, err)
	}

	// The mailbox projection may lag, but public reads must consult the
	// PlayerAuthority receipt once and never advertise a second claim.
	detail := executeMailServiceCommandForTest(t, service, "mail-detail-after-claim", commonpb.CommandType_GET_MAIL_DETAIL, &publicpb.GetMailDetailRequest{MailId: entry.MailID})
	if detail.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("detail after pending projection=%+v", detail)
	}
	var detailResult publicpb.GetMailDetailResult
	if err := proto.Unmarshal(detail.GetResultPayload(), &detailResult); err != nil {
		t.Fatal(err)
	}
	if detailResult.GetMail().GetAttachmentState() != publicpb.MailAttachmentState_MAIL_ATTACHMENT_CLAIMED {
		t.Fatalf("detail attachment state=%s, want CLAIMED", detailResult.GetMail().GetAttachmentState())
	}
	list := executeMailServiceCommandForTest(t, service, "mail-list-after-claim", commonpb.CommandType_GET_MAIL_LIST, &publicpb.GetMailListRequest{})
	if list.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("list after pending projection=%+v", list)
	}
	var listResult publicpb.GetMailListResult
	if err := proto.Unmarshal(list.GetResultPayload(), &listResult); err != nil {
		t.Fatal(err)
	}
	if len(listResult.GetMails()) != 1 || listResult.GetMails()[0].GetAttachmentState() != publicpb.MailAttachmentState_MAIL_ATTACHMENT_CLAIMED {
		t.Fatalf("list after pending projection=%s", listResult.String())
	}
	if listResult.GetSummary().GetClaimableAttachmentCount() != 0 ||
		listResult.GetSummary().GetHasRedPoint() != (listResult.GetSummary().GetUnreadCount() > 0) {
		t.Fatalf("authority overlay left stale claim summary/redpoint: %s", listResult.String())
	}
	if detailResult.GetSummary().GetClaimableAttachmentCount() != 0 ||
		detailResult.GetSummary().GetHasRedPoint() != (detailResult.GetSummary().GetUnreadCount() > 0) {
		t.Fatalf("authority overlay left stale detail summary/redpoint: %s", detailResult.String())
	}

	// Delete binds a trusted proof to the same mail/hash. It must succeed even
	// before the asynchronous mailbox projection is repaired.
	deleted := executeMailServiceCommandForTest(t, service, "mail-delete-after-claim", commonpb.CommandType_DELETE_MAIL, &publicpb.DeleteMailRequest{MailId: entry.MailID})
	if deleted.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("delete claimed mail with pending projection=%+v", deleted)
	}
	_, deletedState, err := mailStore.GetEntry(context.Background(), 9, entry.MailID)
	if err != nil || deletedState.DeletedAtMs == 0 {
		t.Fatalf("claimed mail was not deleted: state=%+v err=%v", deletedState, err)
	}
}

func TestMailClaimReplayWinsAfterClaimWindowExpires(t *testing.T) {
	provider, _ := ar1IngressProvider(t, "memory", 9)
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	service := playeringress.NewService(registry)
	nowMs := time.Now().UnixMilli()
	serviceNow := nowMs
	if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, func() int64 { return serviceNow }); err != nil {
		t.Fatal(err)
	}
	mailStore, err := mail.NewMemoryStore(func() int64 { return serviceNow }, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetMailRepositories(mailStore, mailStore); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{PlayerId: 9, BootstrapId: "mail-claim-replay-expired"}); err != nil {
		t.Fatal(err)
	}
	entry := buildMailClaimEntry(t, nowMs)
	if _, err := mailStore.InsertMailIfAbsent(context.Background(), entry); err != nil {
		t.Fatal(err)
	}
	first := executeMailClaimForTest(t, service, "mail-claim-expiry-replay-op", entry.MailID)
	if first.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("first claim=%+v", first)
	}
	serviceNow = entry.ClaimEndAtMs + 1
	replay := executeMailClaimForTest(t, service, "mail-claim-expiry-replay-op", entry.MailID)
	if replay.GetStatus() != commonpb.CommandStatus_SUCCEEDED || string(replay.GetResultPayload()) != string(first.GetResultPayload()) {
		t.Fatalf("expired replay=%+v, want original terminal", replay)
	}
	newOperation := executeMailClaimForTest(t, service, "mail-claim-expiry-new-op", entry.MailID)
	if newOperation.GetStatus() != commonpb.CommandStatus_REJECTED || newOperation.GetError().GetErrorCode() != commonpb.ErrorCode_ERR_MAIL_CLAIM_EXPIRED {
		t.Fatalf("new operation after claim end=%+v", newOperation)
	}
}

func TestEnsureAndBootstrapDoNotAttachWelcomeMail(t *testing.T) {
	provider, store := ar1IngressProvider(t, "memory", 9)
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	service := playeringress.NewService(registry)
	serviceNow := time.Now().UnixMilli()
	if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, func() int64 { return serviceNow }); err != nil {
		t.Fatal(err)
	}
	mailStore, err := mail.NewMemoryStore(func() int64 { return serviceNow }, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetMailRepositories(mailStore, mailStore); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{PlayerId: 9, BootstrapId: "welcome-mail-player"}); err != nil {
		t.Fatal(err)
	}
	memoryStore, ok := store.(*authoritystore.MemoryStore)
	if !ok {
		t.Fatalf("authority store type=%T", store)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 9}
	if got := memoryStore.AttachedOrderCount(address); got != 0 {
		t.Fatalf("ensure welcome source orders=%d, want 0", got)
	}
	page, err := mailStore.ListPage(context.Background(), 9, "", serviceNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("ensure must not insert welcome mail, page=%+v", page)
	}

	first := executePlayerCommandForTest(t, service, "welcome-bootstrap-1", commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{})
	if first.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("first bootstrap=%+v", first)
	}
	var firstResult publicpb.GetPlayerBootstrapResult
	if err := proto.Unmarshal(first.GetResultPayload(), &firstResult); err != nil {
		t.Fatal(err)
	}
	if got := firstResult.GetFeatureSummary().GetMail(); got.GetUnreadCount() != 0 || got.GetHasRedPoint() {
		t.Fatalf("home bootstrap must not invent welcome mail summary=%+v", got)
	}
	page, err = mailStore.ListPage(context.Background(), 9, "", serviceNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 0 {
		t.Fatalf("bootstrap must not insert welcome mail, page=%+v", page)
	}
	record, versionAfterFirst, err := registry.Read(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetWelcomeMailVersion() != 0 {
		t.Fatalf("welcome_mail_version=%d, want 0", record.GetWelcomeMailVersion())
	}
	if got := memoryStore.AttachedOrderCount(address); got != 0 {
		t.Fatalf("welcome source orders=%d, want 0", got)
	}

	replay := executePlayerCommandForTest(t, service, "welcome-bootstrap-2", commonpb.CommandType_GET_PLAYER_BOOTSTRAP, &publicpb.GetPlayerBootstrapRequest{})
	if replay.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
		t.Fatalf("replay bootstrap=%+v", replay)
	}
	_, versionAfterReplay, err := registry.Read(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	if versionAfterReplay != versionAfterFirst {
		t.Fatalf("duplicate bootstrap advanced version: first=%d replay=%d", versionAfterFirst, versionAfterReplay)
	}
}

func TestEnsureOwnerInitializedReplayDoesNotAttachWelcome(t *testing.T) {
	provider, store := ar1IngressProvider(t, "memory", 9)
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	service := playeringress.NewService(registry)
	var clock atomic.Int64
	clock.Store(time.Now().UnixMilli())
	if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, func() int64 { return clock.Add(11) }); err != nil {
		t.Fatal(err)
	}
	req := &logicrpcpb.EnsureOwnerInitializedRequest{PlayerId: 9, BootstrapId: "welcome-replay-player"}
	if _, err := service.EnsureOwnerInitialized(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := service.EnsureOwnerInitialized(context.Background(), req); err != nil {
		t.Fatalf("returning ensure: %v", err)
	}
	memoryStore, ok := store.(*authoritystore.MemoryStore)
	if !ok {
		t.Fatalf("authority store type=%T", store)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 9}
	if got := memoryStore.AttachedOrderCount(address); got != 0 {
		t.Fatalf("ensure replay attached %d source orders, want 0", got)
	}
}

func TestEnsureAndBootstrapConcurrentWelcomeMailHasOneSourceOrder(t *testing.T) {
	provider, store := ar1IngressProvider(t, "memory", 9)
	registry := playeractor.NewAuthorityRegistry(provider, playeractor.RegistryOptions{MailboxSize: 8})
	t.Cleanup(registry.Stop)
	service := playeringress.NewService(registry)
	if _, err := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{
		PlayerId: 9, BootstrapId: "welcome-race-player",
	}); err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(time.Now().UnixMilli())
	nowMs := func() int64 { return clock.Add(11) }
	if err := service.SetBootstrapTaskDomain(playertask.MustNewP0Repository(), playertask.DefaultGameTimezone, nowMs); err != nil {
		t.Fatal(err)
	}
	mailStore, err := mail.NewMemoryStore(nowMs, mail.LimitsV1())
	if err != nil {
		t.Fatal(err)
	}
	if err := service.SetMailRepositories(mailStore, mailStore); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, ensureErr := service.EnsureOwnerInitialized(context.Background(), &logicrpcpb.EnsureOwnerInitializedRequest{
			PlayerId: 9, BootstrapId: "welcome-race-player",
		})
		errs <- ensureErr
	}()
	go func() {
		defer wg.Done()
		payload, marshalErr := proto.Marshal(&publicpb.GetPlayerBootstrapRequest{})
		if marshalErr != nil {
			errs <- marshalErr
			return
		}
		response, execErr := service.ExecuteCommand(context.Background(), &logicrpcpb.ExecuteCommandRequest{
			Command: &commonpb.ClientCommandEnvelope{
				OperationId: "welcome-race-bootstrap",
				RequestId:   "request-welcome-race-bootstrap",
				CommandType: commonpb.CommandType_GET_PLAYER_BOOTSTRAP,
				Payload:     payload,
			},
			Trusted: &logicrpcpb.TrustedCommandContext{
				Session:                     &logicrpcpb.SessionPermitSnapshot{PlayerId: 9, ConnectionId: "welcome-race"},
				RecoveryCompleteAtAdmission: true,
			},
			Dispatch: &logicrpcpb.DispatchContext{
				RouteMode:  logicrpcpb.RouteMode_ROUTE_MODE_ACTOR_AFFINITY,
				ActorRoute: &logicrpcpb.ActorRouteHeader{RouteDomain: logicrpcpb.RouteDomain_ROUTE_DOMAIN_PLAYER, ActorId: 9},
			},
		})
		if execErr != nil {
			errs <- execErr
			return
		}
		if business := response.GetBusiness(); business.GetStatus() != commonpb.CommandStatus_SUCCEEDED {
			msg := "bootstrap not succeeded"
			if business.GetError().GetErrorMessage() != "" {
				msg = business.GetError().GetErrorMessage()
			}
			errs <- errors.New(msg)
			return
		}
		errs <- nil
	}()
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}

	memoryStore, ok := store.(*authoritystore.MemoryStore)
	if !ok {
		t.Fatalf("authority store type=%T", store)
	}
	address := authoritystore.ActorAddress{Domain: actorrouting.DomainPlayer, ActorID: 9}
	if got := memoryStore.AttachedOrderCount(address); got != 0 {
		t.Fatalf("concurrent welcome source orders=%d, want 0", got)
	}
	record, _, err := registry.Read(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	if record.GetWelcomeMailVersion() != 0 {
		t.Fatalf("welcome_mail_version=%d, want 0", record.GetWelcomeMailVersion())
	}
}
