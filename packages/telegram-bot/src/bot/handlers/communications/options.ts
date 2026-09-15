import { InlineKeyboard } from 'grammy';

/** Marks the primary option, always the first entry of a communication's options. */
export const PRIMARY_OPTION_ICON = '✅';

/** Marks the secondary option, the one users tap to opt out. */
export const SECONDARY_OPTION_ICON = '❌';

/** Callback data prefix that separates option taps from other bot callbacks. */
export const COMMUNICATION_OPTION_PREFIX = 'comm';

/** Matches the callback data attached to every communication option button. */
export const COMMUNICATION_OPTION_PATTERN = /^comm:(\d+):(\d+)$/u;

export interface ParsedCommunicationOptionCallback {
  communicationId: number;
  optionIndex: number;
}

/**
 * Builds the callback payload carried by one option button.
 * Only ids travel with the button because Telegram caps callback data at 64
 * bytes, and the index keeps pointing at the right option even if an admin
 * later edits the labels.
 */
export function buildCommunicationOptionCallbackData(communicationId: number, optionIndex: number) {
  return `${COMMUNICATION_OPTION_PREFIX}:${communicationId}:${optionIndex}`;
}

/**
 * Reads back the communication and option a tapped button refers to.
 */
export function parseCommunicationOptionCallbackData(
  data: string | undefined,
): ParsedCommunicationOptionCallback | null {
  const match = data?.match(COMMUNICATION_OPTION_PATTERN);

  if (!match) return null;

  return {
    communicationId: Number.parseInt(match[1], 10),
    optionIndex: Number.parseInt(match[2], 10),
  };
}

/**
 * Renders the option buttons of a communication as a single keyboard row.
 * Options are stored in semantic order, primary first, but Telegram users
 * expect the confirming action on the right, so the row is reversed at render
 * time while the callback data keeps the stored index.
 */
export function buildCommunicationOptionsKeyboard(communicationId: number, options: string[]) {
  const buttons = options.map((label, optionIndex) => ({
    text: `${optionIndex === 0 ? PRIMARY_OPTION_ICON : SECONDARY_OPTION_ICON} ${label}`,
    callbackData: buildCommunicationOptionCallbackData(communicationId, optionIndex),
  }));

  const keyboard = new InlineKeyboard();

  for (const button of buttons.reverse()) {
    keyboard.text(button.text, button.callbackData);
  }

  return keyboard;
}
