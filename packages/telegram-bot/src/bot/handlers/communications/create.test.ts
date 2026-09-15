import assert from 'node:assert/strict';
import test from 'node:test';

import { parseCreateCommunicationCommand } from './create-parser.js';

test('parseCreateCommunicationCommand parses description and message separated by double greater-than', () => {
  // This input matches the admin command format used to create pending communications.
  const commandText = '/create_communication Maintenance notice >> Validators will be monitored.';

  // This call extracts the communication fields without sending the communication.
  const parsed = parseCreateCommunicationCommand(commandText);

  // This assertion verifies both sides of the separator are trimmed and returned.
  assert.deepEqual(parsed, {
    description: 'Maintenance notice',
    message: 'Validators will be monitored.',
    options: [],
  });
});

test('parseCreateCommunicationCommand rejects commands without the double greater-than separator', () => {
  // This input omits the required separator between description and message.
  const commandText = '/create_communication Maintenance notice Validators will be monitored.';

  // This call validates the command format before any API request is made.
  const parsed = parseCreateCommunicationCommand(commandText);

  // This assertion verifies invalid input is represented as a missing parse result.
  assert.equal(parsed, null);
});

test('parseCreateCommunicationCommand rejects commands with an empty message', () => {
  // This input includes the separator but leaves the message side empty.
  const commandText = '/create_communication Maintenance notice >>   ';

  // This call validates that both required communication fields exist.
  const parsed = parseCreateCommunicationCommand(commandText);

  // This assertion verifies incomplete input is rejected.
  assert.equal(parsed, null);
});

// Admins turn a communication into a question by appending a third block with
// the two button labels, written primary first.
test('parseCreateCommunicationCommand parses the optional button labels', () => {
  // This input asks users to confirm a migration with an accept and a cancel button.
  const commandText =
    '/create_communication Cluster migration >> Do we migrate your cluster? >> Aceptar | Cancelar';

  // This call extracts the message plus the labels the keyboard will render.
  const parsed = parseCreateCommunicationCommand(commandText);

  // This assertion verifies the options keep the order the admin wrote, which is
  // the semantic order stored in the database: index 0 is the primary action.
  assert.deepEqual(parsed, {
    description: 'Cluster migration',
    message: 'Do we migrate your cluster?',
    options: ['Aceptar', 'Cancelar'],
  });
});

// The separator is also valid message content, so a trailing block only becomes
// options when it really looks like a two label list.
test('parseCreateCommunicationCommand keeps a trailing block as message when it is not a label pair', () => {
  // This message quotes the separator and would break if the parser split blindly.
  const commandText = '/create_communication Release notes >> New dashboard >> read the changelog';

  // This call must treat everything after the first separator as the message.
  const parsed = parseCreateCommunicationCommand(commandText);

  // This assertion verifies no buttons are configured and no text is lost.
  assert.deepEqual(parsed, {
    description: 'Release notes',
    message: 'New dashboard >> read the changelog',
    options: [],
  });
});

// A half written options block is a typo, not a request for one button, so the
// whole block stays part of the message rather than silently creating a keyboard.
test('parseCreateCommunicationCommand rejects an options block with an empty label', () => {
  // This input leaves the second label empty after the pipe.
  const commandText = '/create_communication Survey >> Do you like it? >> Si |';

  // This call decides between options and plain message content.
  const parsed = parseCreateCommunicationCommand(commandText);

  // This assertion verifies the malformed block is preserved as message text.
  assert.deepEqual(parsed, {
    description: 'Survey',
    message: 'Do you like it? >> Si |',
    options: [],
  });
});
