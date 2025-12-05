-- CreateEnum
CREATE TYPE "public"."ValidatorExitEvent" AS ENUM ('voluntary', 'slashed');

-- CreateTable
CREATE TABLE "public"."validator" (
    "id" INTEGER NOT NULL,
    "status" INTEGER,
    "balance" BIGINT NOT NULL,
    "effective_balance" BIGINT,
    "pubkey" VARCHAR(98),
    "withdrawal_address" VARCHAR(42),

    CONSTRAINT "validator_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."validator_withdrawals" (
    "slot" INTEGER NOT NULL,
    "validator_index" VARCHAR(98) NOT NULL,
    "amount" BIGINT NOT NULL,

    CONSTRAINT "validator_withdrawals_pkey" PRIMARY KEY ("slot","validator_index")
);

-- CreateTable
CREATE TABLE "public"."validator_deposits" (
    "slot" INTEGER NOT NULL,
    "pubkey" VARCHAR(98) NOT NULL,
    "withdrawal_credentials" VARCHAR(66) NOT NULL,
    "amount" BIGINT NOT NULL,
    "index" INTEGER,

    CONSTRAINT "validator_deposits_pkey" PRIMARY KEY ("slot","pubkey")
);

-- CreateTable
CREATE TABLE "public"."validator_voluntary_exits" (
    "index" INTEGER NOT NULL,
    "epoch" INTEGER NOT NULL,
    "slot" INTEGER NOT NULL,
    "event" "public"."ValidatorExitEvent" NOT NULL,

    CONSTRAINT "validator_voluntary_exits_pkey" PRIMARY KEY ("index")
);

-- CreateTable
CREATE TABLE "public"."validator_request_withdrawals" (
    "slot" INTEGER NOT NULL,
    "pub_key" VARCHAR(98) NOT NULL,
    "amount" BIGINT NOT NULL,

    CONSTRAINT "validator_request_withdrawals_pkey" PRIMARY KEY ("slot","pub_key")
);

-- CreateTable
CREATE TABLE "public"."validator_request_consolidations" (
    "slot" INTEGER NOT NULL,
    "source_pubkey" VARCHAR(98) NOT NULL,
    "target_pubkey" VARCHAR(98) NOT NULL,

    CONSTRAINT "validator_request_consolidations_pkey" PRIMARY KEY ("slot","source_pubkey","target_pubkey")
);

-- CreateTable
CREATE TABLE "public"."epoch" (
    "epoch" INTEGER NOT NULL,
    "processed" BOOLEAN NOT NULL DEFAULT false,
    "all_slots_processed" BOOLEAN NOT NULL DEFAULT false,
    "committees_fetched" BOOLEAN NOT NULL DEFAULT false,
    "sync_committees_fetched" BOOLEAN NOT NULL DEFAULT false,
    "validator_proposer_duties_fetched" BOOLEAN NOT NULL DEFAULT false,
    "validators_balances_fetched" BOOLEAN NOT NULL DEFAULT false,
    "validators_activation_fetched" BOOLEAN NOT NULL DEFAULT false,
    "rewards_fetched" BOOLEAN NOT NULL DEFAULT false,

    CONSTRAINT "epoch_pkey" PRIMARY KEY ("epoch")
);

-- CreateTable
CREATE TABLE "public"."slot" (
    "slot" INTEGER NOT NULL,
    "proposer_index" INTEGER,
    "block_number" INTEGER,
    "consensus_reward" BIGINT,
    "execution_reward" BIGINT,
    "committees_count_in_slot" JSONB,
    "attestations_fetched" BOOLEAN NOT NULL DEFAULT false,
    "sync_rewards_fetched" BOOLEAN NOT NULL DEFAULT false,
    "consensus_rewards_fetched" BOOLEAN NOT NULL DEFAULT false,
    "execution_rewards_fetched" BOOLEAN NOT NULL DEFAULT false,
    "proposer_slashings_fetched" BOOLEAN NOT NULL DEFAULT false,
    "attester_slashings_fetched" BOOLEAN NOT NULL DEFAULT false,
    "deposits_fetched" BOOLEAN NOT NULL DEFAULT false,
    "voluntary_exits_fetched" BOOLEAN NOT NULL DEFAULT false,
    "ep_withdrawals_fetched" BOOLEAN NOT NULL DEFAULT false,
    "er_deposits_fetched" BOOLEAN NOT NULL DEFAULT false,
    "er_withdrawals_fetched" BOOLEAN NOT NULL DEFAULT false,
    "er_consolidations_fetched" BOOLEAN NOT NULL DEFAULT false,
    "processed" BOOLEAN NOT NULL DEFAULT false,

    CONSTRAINT "slot_pkey" PRIMARY KEY ("slot")
);

-- CreateTable
CREATE TABLE "public"."committee" (
    "slot" INTEGER NOT NULL,
    "index" SMALLINT NOT NULL,
    "validator_index" INTEGER NOT NULL,
    "aggregation_bits_index" SMALLINT NOT NULL,
    "attestation_delay" SMALLINT,

    CONSTRAINT "committee_pkey" PRIMARY KEY ("slot","index","aggregation_bits_index")
);

-- CreateTable
CREATE TABLE "public"."sync_committee" (
    "from_epoch" INTEGER NOT NULL,
    "to_epoch" INTEGER NOT NULL,
    "validators" JSONB NOT NULL,
    "validator_aggregates" JSONB NOT NULL,

    CONSTRAINT "sync_committee_pkey" PRIMARY KEY ("from_epoch","to_epoch")
);

-- CreateTable
CREATE TABLE "public"."sync_committee_rewards" (
    "slot" INTEGER NOT NULL,
    "validator_index" INTEGER NOT NULL,
    "sync_committee_reward" BIGINT NOT NULL,

    CONSTRAINT "sync_committee_rewards_pkey" PRIMARY KEY ("slot","validator_index")
);

-- CreateTable
CREATE TABLE "public"."epoch_rewards" (
    "epoch" INTEGER NOT NULL,
    "validator_index" INTEGER NOT NULL,
    "head" BIGINT NOT NULL,
    "target" BIGINT NOT NULL,
    "source" BIGINT NOT NULL,
    "inactivity" BIGINT NOT NULL,
    "missed_head" BIGINT NOT NULL,
    "missed_target" BIGINT NOT NULL,
    "missed_source" BIGINT NOT NULL,
    "missed_inactivity" BIGINT NOT NULL,

    CONSTRAINT "epoch_rewards_pkey" PRIMARY KEY ("epoch","validator_index")
);

-- CreateTable
CREATE TABLE "public"."hourly_validator_stats" (
    "datetime" TIMESTAMP NOT NULL,
    "validator_index" INTEGER NOT NULL,
    "missed_attestations_count" SMALLINT,
    "cl_rewards" BIGINT,
    "cl_missed_rewards" BIGINT,

    CONSTRAINT "hourly_validator_stats_pkey" PRIMARY KEY ("datetime","validator_index")
);

-- CreateTable
CREATE TABLE "public"."user" (
    "id" BIGINT NOT NULL,
    "login_id" TEXT NOT NULL,
    "user_id" BIGINT NOT NULL,
    "chat_id" BIGINT NOT NULL,
    "username" TEXT NOT NULL,
    "message_id" BIGINT,
    "last_claimed" TIMESTAMP(3),
    "has_blocked_bot" BOOLEAN NOT NULL DEFAULT false,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "performance_notif" TIMESTAMP(3),
    "performance_threshold" INTEGER NOT NULL DEFAULT 90,
    "inactive_notif" TIMESTAMP(3),
    "inactive_on_missed_attestations" INTEGER NOT NULL DEFAULT 3,

    CONSTRAINT "user_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."withdrawal_address" (
    "address" TEXT NOT NULL,

    CONSTRAINT "withdrawal_address_pkey" PRIMARY KEY ("address")
);

-- CreateTable
CREATE TABLE "public"."fee_reward_address" (
    "address" TEXT NOT NULL,
    "user_id" BIGINT,

    CONSTRAINT "fee_reward_address_pkey" PRIMARY KEY ("address")
);

-- CreateTable
CREATE TABLE "public"."DailyValidatorStats" (
    "validator_index" INTEGER NOT NULL,
    "date" DATE NOT NULL,
    "head" BIGINT,
    "target" BIGINT,
    "source" BIGINT,
    "inactivity" BIGINT,
    "sync_committee" BIGINT,
    "block_reward" BIGINT,
    "missed_head" BIGINT,
    "missed_target" BIGINT,
    "missed_source" BIGINT,
    "missed_inactivity" BIGINT,
    "attestations_missed" INTEGER,

    CONSTRAINT "DailyValidatorStats_pkey" PRIMARY KEY ("validator_index","date")
);

-- CreateTable
CREATE TABLE "public"."last_summary_update" (
    "id" SERIAL NOT NULL,
    "hourly_validator_stats" TIMESTAMP(3),
    "daily_validator_stats" DATE,
    "weekly_validator_stats" TIMESTAMP(3),
    "monthly_validator_stats" TIMESTAMP(3),
    "yearly_validator_stats" TIMESTAMP(3),

    CONSTRAINT "last_summary_update_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "public"."ValidatorsStats" (
    "validatorId" INTEGER NOT NULL,
    "validatorStatus" INTEGER,
    "oneHourMissed" INTEGER,
    "lastMissed" INTEGER[],
    "dailyCLRewards" BIGINT,
    "dailyELRewards" BIGINT,
    "weeklyCLRewards" BIGINT,
    "weeklyELRewards" BIGINT,
    "monthlyCLRewards" BIGINT,
    "monthlyELRewards" BIGINT,
    "timestamp" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "ValidatorsStats_pkey" PRIMARY KEY ("validatorId")
);

-- CreateTable
CREATE TABLE "public"."HourlyBlockAndSyncRewards" (
    "validatorIndex" INTEGER NOT NULL,
    "hour" INTEGER NOT NULL,
    "date" DATE NOT NULL,
    "blockReward" BIGINT,
    "syncCommittee" BIGINT,

    CONSTRAINT "HourlyBlockAndSyncRewards_pkey" PRIMARY KEY ("validatorIndex","date","hour")
);

-- CreateTable
CREATE TABLE "public"."ExecutionRewards" (
    "address" TEXT NOT NULL,
    "timestamp" TIMESTAMP(3) NOT NULL,
    "amount" DECIMAL(78,0) NOT NULL,
    "blockNumber" INTEGER NOT NULL,

    CONSTRAINT "ExecutionRewards_pkey" PRIMARY KEY ("blockNumber")
);

-- CreateTable
CREATE TABLE "public"."beacon_daily_validator_stats" (
    "date" DATE NOT NULL,
    "pending_queued" INTEGER NOT NULL DEFAULT 0,
    "active_ongoing" INTEGER NOT NULL DEFAULT 0,
    "active_exiting" INTEGER NOT NULL DEFAULT 0,
    "avg_balance" BIGINT,
    "avg_effective_balance" BIGINT,

    CONSTRAINT "beacon_daily_validator_stats_pkey" PRIMARY KEY ("date")
);

-- CreateTable
CREATE TABLE "public"."_user_to_validator" (
    "A" BIGINT NOT NULL,
    "B" INTEGER NOT NULL,

    CONSTRAINT "_user_to_validator_AB_pkey" PRIMARY KEY ("A","B")
);

-- CreateTable
CREATE TABLE "public"."_user_to_withdrawal_address" (
    "A" BIGINT NOT NULL,
    "B" TEXT NOT NULL,

    CONSTRAINT "_user_to_withdrawal_address_AB_pkey" PRIMARY KEY ("A","B")
);

-- CreateTable
CREATE TABLE "public"."_user_to_fee_reward_address" (
    "A" TEXT NOT NULL,
    "B" BIGINT NOT NULL,

    CONSTRAINT "_user_to_fee_reward_address_AB_pkey" PRIMARY KEY ("A","B")
);

-- CreateIndex
CREATE INDEX "validator_status_idx" ON "public"."validator"("status");

-- CreateIndex
CREATE INDEX "validator_pubkey_idx" ON "public"."validator"("pubkey");

-- CreateIndex
CREATE INDEX "slot_slot_processed_idx" ON "public"."slot"("slot", "processed");

-- CreateIndex
CREATE INDEX "committee_slot_validator_index_attestation_delay_idx" ON "public"."committee"("slot", "validator_index", "attestation_delay");

-- CreateIndex
CREATE UNIQUE INDEX "user_login_id_key" ON "public"."user"("login_id");

-- CreateIndex
CREATE UNIQUE INDEX "user_user_id_key" ON "public"."user"("user_id");

-- CreateIndex
CREATE UNIQUE INDEX "user_chat_id_key" ON "public"."user"("chat_id");

-- CreateIndex
CREATE UNIQUE INDEX "user_username_key" ON "public"."user"("username");

-- CreateIndex
CREATE INDEX "DailyValidatorStats_date_idx" ON "public"."DailyValidatorStats"("date");

-- CreateIndex
CREATE INDEX "HourlyBlockAndSyncRewards_date_hour_idx" ON "public"."HourlyBlockAndSyncRewards"("date", "hour");

-- CreateIndex
CREATE INDEX "ExecutionRewards_timestamp_address_idx" ON "public"."ExecutionRewards"("timestamp", "address");

-- CreateIndex
CREATE INDEX "_user_to_validator_B_index" ON "public"."_user_to_validator"("B");

-- CreateIndex
CREATE INDEX "_user_to_withdrawal_address_B_index" ON "public"."_user_to_withdrawal_address"("B");

-- CreateIndex
CREATE INDEX "_user_to_fee_reward_address_B_index" ON "public"."_user_to_fee_reward_address"("B");

-- AddForeignKey
ALTER TABLE "public"."_user_to_validator" ADD CONSTRAINT "_user_to_validator_A_fkey" FOREIGN KEY ("A") REFERENCES "public"."user"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."_user_to_validator" ADD CONSTRAINT "_user_to_validator_B_fkey" FOREIGN KEY ("B") REFERENCES "public"."validator"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."_user_to_withdrawal_address" ADD CONSTRAINT "_user_to_withdrawal_address_A_fkey" FOREIGN KEY ("A") REFERENCES "public"."user"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."_user_to_withdrawal_address" ADD CONSTRAINT "_user_to_withdrawal_address_B_fkey" FOREIGN KEY ("B") REFERENCES "public"."withdrawal_address"("address") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."_user_to_fee_reward_address" ADD CONSTRAINT "_user_to_fee_reward_address_A_fkey" FOREIGN KEY ("A") REFERENCES "public"."fee_reward_address"("address") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "public"."_user_to_fee_reward_address" ADD CONSTRAINT "_user_to_fee_reward_address_B_fkey" FOREIGN KEY ("B") REFERENCES "public"."user"("id") ON DELETE CASCADE ON UPDATE CASCADE;
