-- AlterTable
ALTER TABLE "public"."communication" ADD COLUMN "options" TEXT[] DEFAULT ARRAY[]::TEXT[];

-- CreateTable
CREATE TABLE "public"."communication_response" (
    "id" SERIAL NOT NULL,
    "communication_id" INTEGER NOT NULL,
    "telegram_id" TEXT NOT NULL,
    "option_index" INTEGER NOT NULL,
    "responded_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "communication_response_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE UNIQUE INDEX "communication_response_communication_id_telegram_id_key"
  ON "public"."communication_response"("communication_id", "telegram_id");

-- AddForeignKey
ALTER TABLE "public"."communication_response"
  ADD CONSTRAINT "communication_response_communication_id_fkey"
  FOREIGN KEY ("communication_id") REFERENCES "public"."communication"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;
