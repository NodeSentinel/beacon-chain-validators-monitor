import type { Context } from '@/src/bot/context.js'
import { Composer } from 'grammy'
import { logHandle } from '@/src/bot/helpers/logging.js'

const composer = new Composer<Context>()

const feature = composer.chatType('private')

feature.command('start', logHandle('command-start'), (ctx) => {
  return ctx.reply(ctx.t('welcome'))
})

export { composer as welcomeFeature }
