import { chatAction } from '@grammyjs/auto-chat-action';
import { Composer } from 'grammy';

import type { Context } from '@/src/bot/context.js';
import { isAdmin } from '@/src/bot/filters/is-admin.js';
import { setCommandsHandler } from '@/src/bot/handlers/commands/setcommands.js';
import { logHandle } from '@/src/bot/helpers/logging.js';

const composer = new Composer<Context>();

const feature = composer.chatType('private').filter(isAdmin);

feature.command(
  'setcommands',
  logHandle('command-setcommands'),
  chatAction('typing'),
  setCommandsHandler,
);

export { composer as adminFeature };
