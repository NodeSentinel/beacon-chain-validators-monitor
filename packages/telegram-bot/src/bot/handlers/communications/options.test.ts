import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildCommunicationOptionsKeyboard,
  parseCommunicationOptionCallbackData,
} from './options.js';

// The stored options are ordered primary first, but Telegram users expect the
// confirming button on the right, so the rendered row is the reverse.
test('buildCommunicationOptionsKeyboard renders cancel first and keeps the stored indexes', () => {
  // Communication 12 offers "Aceptar" as primary option (index 0) and "Cancelar" as secondary (index 1).
  const keyboard = buildCommunicationOptionsKeyboard(12, ['Aceptar', 'Cancelar']);

  // This assertion verifies both buttons share one row, that the cancel button
  // is rendered on the left, and that each callback payload still points at the
  // semantic index of the option rather than at its position in the row.
  assert.deepEqual(keyboard.inline_keyboard, [
    [
      { text: '❌ Cancelar', callback_data: 'comm:12:1' },
      { text: '✅ Aceptar', callback_data: 'comm:12:0' },
    ],
  ]);
});

// The callback payload is the only thing that travels back when a user taps.
test('parseCommunicationOptionCallbackData reads the communication and option ids', () => {
  // This payload is what Telegram sends back for the primary button of communication 12.
  const parsed = parseCommunicationOptionCallbackData('comm:12:0');

  // This assertion verifies the handler can attribute the answer to the right option.
  assert.deepEqual(parsed, { communicationId: 12, optionIndex: 0 });
});

// Other features use the same callback channel, so foreign payloads must be ignored.
test('parseCommunicationOptionCallbackData ignores callbacks from other features', () => {
  // This payload belongs to the dismiss button shipped with plain communications.
  const parsed = parseCommunicationOptionCallbackData('remove_message');

  // This assertion verifies unrelated callbacks never produce a stored answer.
  assert.equal(parsed, null);
});
