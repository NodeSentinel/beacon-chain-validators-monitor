import type { BotConfig } from 'grammy'
import type { Context } from '@/src/bot/context.js'
import type { Config } from '@/src/config.js'
import type { Logger } from '@/src/logger.js'
import { autoChatAction } from '@grammyjs/auto-chat-action'
import { hydrate } from '@grammyjs/hydrate'
import { hydrateReply, parseMode } from '@grammyjs/parse-mode'
import { sequentialize } from '@grammyjs/runner'
import { MemorySessionStorage, Bot as TelegramBot } from 'grammy'
import { adminFeature } from '@/src/bot/features/admin.js'
import { languageFeature } from '@/src/bot/features/language.js'
import { unhandledFeature } from '@/src/bot/features/unhandled.js'
import { welcomeFeature } from '@/src/bot/features/welcome.js'
import { errorHandler } from '@/src/bot/handlers/error.js'
import { i18n, isMultipleLocales } from '@/src/bot/i18n.js'
import { session } from '@/src/bot/middlewares/session.js'
import { updateLogger } from '@/src/bot/middlewares/update-logger.js'

interface Dependencies {
  config: Config
  logger: Logger
}

function getSessionKey(ctx: Omit<Context, 'session'>) {
  return ctx.chat?.id.toString()
}

export function createBot(token: string, dependencies: Dependencies, botConfig?: BotConfig<Context>) {
  const {
    config,
    logger,
  } = dependencies

  const bot = new TelegramBot<Context>(token, botConfig)

  bot.use(async (ctx, next) => {
    ctx.config = config
    ctx.logger = logger.child({
      update_id: ctx.update.update_id,
    })

    await next()
  })

  const protectedBot = bot.errorBoundary(errorHandler)

  // Middlewares
  bot.api.config.use(parseMode('HTML'))

  if (config.isPollingMode)
    protectedBot.use(sequentialize(getSessionKey))

  if (config.isDebug)
    protectedBot.use(updateLogger())

  protectedBot.use(autoChatAction(bot.api))
  protectedBot.use(hydrateReply)
  protectedBot.use(hydrate())
  protectedBot.use(session({
    getSessionKey,
    storage: new MemorySessionStorage(),
  }))
  protectedBot.use(i18n)

  // Handlers
  protectedBot.use(welcomeFeature)
  protectedBot.use(adminFeature)
  if (isMultipleLocales)
    protectedBot.use(languageFeature)

  // must be the last handler
  protectedBot.use(unhandledFeature)

  return bot
}

export type Bot = ReturnType<typeof createBot>
