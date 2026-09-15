import type { CommandContext } from 'grammy';

import type { Context } from '@/src/bot/context.js';

/**
 * Builds the hidden admin command helper message.
 */
export function getAdminHelpMessage() {
  return [
    '<b>Admin commands</b>',
    '/create_communication &lt;description&gt; &gt;&gt; &lt;message&gt; - create pending communication',
    '/create_communication &lt;description&gt; &gt;&gt; &lt;message&gt; &gt;&gt; &lt;accept&gt; | &lt;cancel&gt; - same, with answer buttons',
    '/list_communications - show pending previews',
    '/send_communication &lt;id&gt; - send pending communication',
    '/communication_results &lt;id&gt; - show answers per button',
    '/setcommands - refresh public commands',
  ].join('\n');
}

/**
 * Replies with the hidden admin command helper.
 */
export function adminHelpHandler(ctx: CommandContext<Context>) {
  return ctx.reply(getAdminHelpMessage());
}
