package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	commonpb "farmgame/api/gen/common"
	publicpb "farmgame/api/gen/public"
	"farmgame/internal/actorrouting"
	"farmgame/internal/cropcatalog"
	"farmgame/internal/identity"
	playerpet "farmgame/internal/player/pet"
	"farmgame/internal/shopcatalog"
	"farmgame/internal/workflow"

	"google.golang.org/protobuf/proto"
)

func (e *e2e) sourceOrderWSSSupportBeforePlant(session *wssSession) error {
	if err := e.waitWelcomeMailSourceOrder(session.playerID); err != nil {
		return err
	}
	if err := session.claimWelcomeMail(); err != nil {
		return err
	}
	if err := session.claimDailyLogin(); err != nil {
		return err
	}
	if err := session.getPetState(false); err != nil {
		return err
	}
	return nil
}

func (e *e2e) sourceOrderWSSSupportAfterHarvest(session *wssSession) error {
	if err := session.getCompendiumUnlocked(uint32(cropcatalog.CarrotDefinitionID)); err != nil {
		return err
	}
	if err := session.sellProduce(); err != nil {
		return err
	}
	if err := session.unlockAndFeedDog(); err != nil {
		return err
	}
	return nil
}

func (e *e2e) waitWelcomeMailSourceOrder(playerID uint64) error {
	orderID, err := welcomeMailSourceOrderID(playerID)
	if err != nil {
		return err
	}
	bucket, err := e.profile.AuthorityBucketForActor(e.calculator, actorrouting.ActorAddress{
		Domain: actorrouting.DomainPlayer, ActorID: playerID,
	})
	if err != nil {
		return fmt.Errorf("welcome mail bucket: %w", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, getErr := e.authority.WorkLease().GetSourceOrder(context.Background(), bucket, string(identity.WorkTypeMailDeliveryOrder), orderID)
		if getErr == nil && snapshot.WorkID != "" {
			log.Printf("SOURCE_ORDER_WSS welcome MAIL_DELIVERY_ORDER attached by DurableCommand state=%s player=%d", snapshot.State, playerID)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("welcome mail source order missing (%s)", e.sourceOrderProbe(
		actorrouting.DomainPlayer,
		string(identity.WorkTypeMailDeliveryOrder),
		orderID,
		playerID,
	))
}

func (e *e2e) assertNoLegacyWorkRecord(workType, workID string) error {
	if e.repository == nil {
		return errors.New("work repository is not composed")
	}
	record, err := e.repository.Get(context.Background(), workType, workID)
	if errors.Is(err, workflow.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	return fmt.Errorf("legacy WorkRecord still exists type=%s id=%s state=%s", workType, workID, record.State)
}

func welcomeMailSourceOrderID(playerID uint64) (string, error) {
	canonical := strconv.FormatUint(playerID, 10)
	return identity.SourceOrderID(
		identity.WorkTypeMailDeliveryOrder,
		identity.SourceKindPlayerActor,
		canonical,
		fmt.Sprintf("WELCOME:%s:1", canonical),
		1,
	)
}

func (s *wssSession) claimWelcomeMail() error {
	list := &publicpb.GetMailListResult{}
	if err := s.roundTrip(commonpb.CommandType_GET_MAIL_LIST, &publicpb.GetMailListRequest{}, list); err != nil {
		return fmt.Errorf("GET_MAIL_LIST: %w", err)
	}
	var mailID string
	for _, item := range list.GetMails() {
		if item.GetAttachmentState() == publicpb.MailAttachmentState_MAIL_ATTACHMENT_CLAIMABLE {
			mailID = item.GetMailId()
			break
		}
	}
	if mailID == "" {
		return fmt.Errorf("welcome mail not claimable mails=%d", len(list.GetMails()))
	}
	claimed := &publicpb.ClaimMailAttachmentResult{}
	if err := s.roundTrip(commonpb.CommandType_CLAIM_MAIL_ATTACHMENT, &publicpb.ClaimMailAttachmentRequest{MailId: mailID}, claimed); err != nil {
		return fmt.Errorf("CLAIM_MAIL_ATTACHMENT: %w", err)
	}
	if claimed.GetGrantedReward() == nil || len(claimed.GetGrantedReward().GetItems()) == 0 {
		return errors.New("welcome mail granted no seed items")
	}
	if ps := claimed.GetPrivateState(); ps != nil {
		s.wallet = ps.GetCoin()
		s.inventory = make(map[int32]int64, len(ps.GetInventory()))
		for _, stack := range ps.GetInventory() {
			s.inventory[stack.GetItemId()] = stack.GetCount()
		}
	}
	log.Printf("SOURCE_ORDER_WSS claimed welcome mail=%s wallet=%d", mailID, s.wallet)
	return nil
}

func (s *wssSession) claimDailyLogin() error {
	list := &publicpb.GetTaskListResult{}
	if err := s.roundTrip(commonpb.CommandType_GET_TASK_LIST, &publicpb.GetTaskListRequest{}, list); err != nil {
		return fmt.Errorf("GET_TASK_LIST: %w", err)
	}
	var instanceID string
	for _, task := range list.GetDailyTasks() {
		progress := task.GetProgress()
		if progress.GetState() == publicpb.PlayerTaskProgressState_PLAYER_TASK_PROGRESS_CLAIMABLE {
			instanceID = progress.GetInstanceId()
			break
		}
	}
	if instanceID == "" {
		return fmt.Errorf("no claimable daily task claimable_count=%d", list.GetSummary().GetClaimableCount())
	}
	claimed := &publicpb.ClaimTaskResult{}
	if err := s.roundTrip(commonpb.CommandType_CLAIM_TASK, &publicpb.ClaimTaskRequest{TaskInstanceId: instanceID}, claimed); err != nil {
		return fmt.Errorf("CLAIM_TASK: %w", err)
	}
	if ps := claimed.GetPrivateState(); ps != nil {
		s.wallet = ps.GetCoin()
		s.exp = ps.GetExp()
	}
	log.Printf("SOURCE_ORDER_WSS claimed daily task=%s exp=%d", instanceID, s.exp)
	return nil
}

func (s *wssSession) getPetState(wantGuarding bool) error {
	result := &publicpb.GetPetStateResult{}
	if err := s.roundTrip(commonpb.CommandType_GET_PET_STATE, &publicpb.GetPetStateRequest{}, result); err != nil {
		return fmt.Errorf("GET_PET_STATE: %w", err)
	}
	page := result.GetPage()
	if page == nil || page.GetState() == nil {
		return errors.New("GET_PET_STATE missing page")
	}
	guarding := page.GetState().GetGuardEffective()
	if wantGuarding && !guarding {
		return fmt.Errorf("pet guard not effective until=%d", page.GetState().GetGuardUntilMs())
	}
	if !wantGuarding && guarding {
		return errors.New("new player already has an effective guard")
	}
	log.Printf("SOURCE_ORDER_WSS GET_PET_STATE owned=%d guarding=%v", len(page.GetState().GetOwnedPetIds()), guarding)
	return nil
}

func (s *wssSession) getCompendiumUnlocked(cropID uint32) error {
	result := &publicpb.GetCompendiumResult{}
	if err := s.roundTrip(commonpb.CommandType_GET_COMPENDIUM, &publicpb.GetCompendiumRequest{}, result); err != nil {
		return fmt.Errorf("GET_COMPENDIUM: %w", err)
	}
	page := result.GetPage()
	if page == nil {
		return errors.New("GET_COMPENDIUM missing page")
	}
	for _, category := range page.GetCategories() {
		for _, entry := range category.GetEntries() {
			if entry.GetCropDefinitionId() == cropID &&
				entry.GetDisplayState() == publicpb.CompendiumDisplayState_COMPENDIUM_UNLOCKED_FULL {
				log.Printf("SOURCE_ORDER_WSS GET_COMPENDIUM unlocked crop=%d revision=%d", cropID, page.GetCompendiumRevision())
				return nil
			}
		}
	}
	return fmt.Errorf("compendium did not unlock crop=%d revision=%d", cropID, page.GetCompendiumRevision())
}

func (s *wssSession) sellProduce() error {
	count := s.inventory[cropcatalog.CarrotItemID]
	if count <= 0 {
		return fmt.Errorf("no carrot produce to sell inventory=%v", s.inventory)
	}
	result := &publicpb.SellResult{}
	if err := s.roundTrip(commonpb.CommandType_SELL, &publicpb.SellRequest{ItemId: cropcatalog.CarrotItemID, Count: count}, result); err != nil {
		return fmt.Errorf("SELL: %w", err)
	}
	if ps := result.GetPrivateState(); ps != nil {
		s.wallet = ps.GetCoin()
		s.inventory = make(map[int32]int64, len(ps.GetInventory()))
		for _, stack := range ps.GetInventory() {
			s.inventory[stack.GetItemId()] = stack.GetCount()
		}
	}
	log.Printf("SOURCE_ORDER_WSS sold carrots count=%d wallet=%d", count, s.wallet)
	return nil
}

func (s *wssSession) unlockAndFeedDog() error {
	if s.wallet < 100 {
		return fmt.Errorf("not enough coin to unlock dog wallet=%d", s.wallet)
	}
	unlock := &publicpb.PetUnlockByCoinResult{}
	if err := s.roundTrip(commonpb.CommandType_PET_UNLOCK_BY_COIN, &publicpb.PetUnlockByCoinRequest{
		PetId: uint32(playerpet.P0DogPetID), ExpectedPetConfigVersion: 1, ExpectedPriceVersion: 1,
	}, unlock); err != nil {
		return fmt.Errorf("PET_UNLOCK_BY_COIN: %w", err)
	}
	if ps := unlock.GetMutation().GetResultingState(); ps != nil && unlock.GetMutation() != nil {
		s.wallet += unlock.GetMutation().GetCoinDelta()
	} else if unlock.GetMutation() != nil {
		s.wallet += unlock.GetMutation().GetCoinDelta()
	}
	switchResult := &publicpb.PetSwitchResult{}
	if err := s.roundTrip(commonpb.CommandType_PET_SWITCH, &publicpb.PetSwitchRequest{
		TargetPetId: uint32(playerpet.P0DogPetID), ExpectedPetConfigVersion: 1, ExpectedGuardRuleVersion: 1,
	}, switchResult); err != nil {
		return fmt.Errorf("PET_SWITCH: %w", err)
	}
	buy := &publicpb.BuyResult{}
	if err := s.roundTrip(commonpb.CommandType_BUY, &publicpb.BuyRequest{
		OfferId: shopcatalog.DogFoodOfferID, Count: 1, ExpectedOfferVersion: shopcatalog.OfferVersionOf(shopcatalog.DogFoodOfferID),
	}, buy); err != nil {
		return fmt.Errorf("BUY dogfood: %w", err)
	}
	if ps := buy.GetPrivateState(); ps != nil {
		s.wallet = ps.GetCoin()
		s.inventory = make(map[int32]int64, len(ps.GetInventory()))
		for _, stack := range ps.GetInventory() {
			s.inventory[stack.GetItemId()] = stack.GetCount()
		}
	}
	if s.inventory[playerpet.P0DogFoodItemID] < 1 {
		return fmt.Errorf("dog food missing after BUY inventory=%v", s.inventory)
	}
	feed := &publicpb.PetFeedResult{}
	if err := s.roundTrip(commonpb.CommandType_PET_FEED, &publicpb.PetFeedRequest{
		Quantity: 1, ExpectedFeedRuleVersion: 1, ExpectedActivePetConfigVersion: 1, ExpectedGuardRuleVersion: 1,
	}, feed); err != nil {
		return fmt.Errorf("PET_FEED: %w", err)
	}
	if err := s.getPetState(true); err != nil {
		return err
	}
	log.Printf("SOURCE_ORDER_WSS pet unlock+switch+dogfood+feed wallet=%d", s.wallet)
	return nil
}

func (s *wssSession) roundTrip(commandType commonpb.CommandType, req proto.Message, dest proto.Message) error {
	op := fmt.Sprintf("so-wss-%s-%d", commandType.String(), time.Now().UnixNano())
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return err
	}
	if err := s.sendCommand(op, "request-"+op, commandType, raw); err != nil {
		return err
	}
	payload, err := s.waitTerminal(op, 20*time.Second)
	if err != nil {
		return err
	}
	if dest == nil {
		return nil
	}
	if err := proto.Unmarshal(payload, dest); err != nil {
		return fmt.Errorf("decode %s: %w", commandType, err)
	}
	return nil
}
